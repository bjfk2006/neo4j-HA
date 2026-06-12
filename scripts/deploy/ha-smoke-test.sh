#!/usr/bin/env bash
set -euo pipefail

# One-click HA smoke test for local/docker deployment.
# Validates:
# 1) Agent health and metrics endpoint
# 2) Cluster state shape and node health
# 3) CDC -> Redis -> SyncApplier replication
# 4) Optional switchover and post-switchover replication

AGENT_URL="${AGENT_URL:-http://localhost:8080}"
COMPOSE_FILE="${COMPOSE_FILE:-docker/neo4j/test-compose.yml}"
WAIT_SECONDS="${WAIT_SECONDS:-10}"
SWITCHOVER_WAIT_SECONDS="${SWITCHOVER_WAIT_SECONDS:-15}"
ENABLE_SWITCHOVER="${ENABLE_SWITCHOVER:-1}"
EXPECTED_STANDBY_COUNT="${EXPECTED_STANDBY_COUNT:-1}"

NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

# Auto-load docker/.env when required credentials are missing.
# Explicitly provided environment variables still take precedence.
if [[ -z "${NEO4J_PASSWORD}" || -z "${ADMIN_TOKEN}" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
  ENV_FILE="${PROJECT_ROOT}/docker/.env"
  if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
    NEO4J_PASSWORD="${NEO4J_PASSWORD:-}"
    ADMIN_TOKEN="${ADMIN_TOKEN:-}"
  fi
fi

log() {
  echo "[$(date +'%H:%M:%S')] $*"
}

die() {
  echo "[ERROR] $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing command: $1"
}

require_cmd docker
require_cmd curl
require_cmd python3

[[ -n "${NEO4J_PASSWORD}" ]] || die "NEO4J_PASSWORD is required"
[[ -n "${ADMIN_TOKEN}" ]] || die "ADMIN_TOKEN is required"

get_cluster_status() {
  curl -fsS "${AGENT_URL}/cluster/status"
}

json_get_primary_node() {
  python3 -c 'import json,sys; print(json.loads(sys.stdin.read()).get("primaryNode",""))'
}

json_get_host_by_node_id() {
  local node_id="$1"
  python3 -c 'import json,sys,re
doc=json.loads(sys.stdin.read() or "{}")
target=sys.argv[1]
for n in doc.get("nodes",[]):
    if n.get("id")==target:
        uri=n.get("boltUri","")
        m=re.match(r"bolt://([^:]+):\d+", uri)
        print(m.group(1) if m else "")
        break' "$node_id"
}

json_get_first_healthy_standby_node() {
  python3 -c 'import json,sys
d=json.loads(sys.stdin.read() or "{}")
for n in d.get("nodes",[]):
    if n.get("role")=="STANDBY" and n.get("health")=="HEALTHY":
        print(n.get("id",""))
        break'
}

json_get_node_service_state() {
  local node_id="$1"
  python3 -c 'import json,sys
doc=json.loads(sys.stdin.read() or "{}")
target=sys.argv[1]
for n in doc.get("nodes",[]):
    if n.get("id")==target:
        print(n.get("serviceState",""))
        break' "$node_id"
}

assert_cluster_healthy() {
  local status_json="$1"
  local expected_standby_count="$2"
  python3 -c 'import json,sys
doc=json.loads(sys.stdin.read() or "{}")
expected=int(sys.argv[1])
nodes=doc.get("nodes",[])
if not nodes:
    raise SystemExit("cluster/status nodes is empty")
primary=doc.get("primaryNode")
if not primary:
    raise SystemExit("cluster/status primaryNode is empty")
primary_node=next((n for n in nodes if n.get("id")==primary), None)
if primary_node is None:
    raise SystemExit("cluster/status primaryNode not found in nodes")
if primary_node.get("health") != "HEALTHY":
    raise SystemExit("primary node is not HEALTHY: " + primary)
healthy_standbys=[n for n in nodes if n.get("role")=="STANDBY" and n.get("health")=="HEALTHY"]
if len(healthy_standbys) < expected:
    raise SystemExit(f"healthy standby nodes {len(healthy_standbys)} < expected {expected}")
print("OK")' "${expected_standby_count}" <<<"${status_json}"
}

run_replication_probe() {
  local write_host="$1"
  local read_host="$2"
  local probe_id="$3"
  local max_retries="${PROBE_READ_RETRIES:-6}"
  local retry_interval="${PROBE_RETRY_INTERVAL:-5}"

  log "Writing probe '${probe_id}' to ${write_host}"
  docker exec "${write_host}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
    "MERGE (n:HAProbe {id:'${probe_id}'}) SET n.ts=timestamp(), n.msg='ha-smoke' RETURN n.id" >/dev/null

  sleep "${WAIT_SECONDS}"

  local attempt=1
  while (( attempt <= max_retries )); do
    log "Reading probe '${probe_id}' from ${read_host} (attempt ${attempt}/${max_retries})"
    local out
    out="$(docker exec "${read_host}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
      "MATCH (n:HAProbe {id:'${probe_id}'}) RETURN n.id" 2>/dev/null || true)"
    if echo "${out}" | grep -q "${probe_id}"; then
      return 0
    fi
    if (( attempt < max_retries )); then
      log "Probe not yet replicated, retrying in ${retry_interval}s..."
      sleep "${retry_interval}"
    fi
    (( attempt++ ))
  done
  die "Replication probe '${probe_id}' not found on ${read_host} after ${max_retries} attempts"
}

log "Checking docker services status..."
docker compose -f "${COMPOSE_FILE}" ps >/dev/null

log "Checking HA Agent health endpoint..."
health_json="$(curl -fsS "${AGENT_URL}/health")"
echo "${health_json}" | grep -q "UP" || die "/health is not UP"

log "Checking metrics endpoint..."
metrics="$(curl -fsS "${AGENT_URL}/metrics")"
echo "${metrics}" | grep -q "neo4j_ha_sync_lag_ms" || die "metrics missing neo4j_ha_sync_lag_ms"
echo "${metrics}" | grep -q "neo4j_ha_cdc_events_published_total" || die "metrics missing cdc counter"

log "Checking cluster status..."
status_json="$(get_cluster_status)"
assert_cluster_healthy "${status_json}" "${EXPECTED_STANDBY_COUNT}" >/dev/null

primary_node="$(echo "${status_json}" | json_get_primary_node)"
[[ -n "${primary_node}" ]] || die "primaryNode is empty"
primary_host="$(echo "${status_json}" | json_get_host_by_node_id "${primary_node}")"
[[ -n "${primary_host}" ]] || die "failed to resolve primary host"

other_node="$(echo "${status_json}" | json_get_first_healthy_standby_node)"
[[ -n "${other_node}" ]] || die "failed to resolve healthy standby node"
other_host="$(echo "${status_json}" | json_get_host_by_node_id "${other_node}")"
[[ -n "${other_host}" ]] || die "failed to resolve healthy standby host"

probe1="ha_probe_$(date +%s)"
run_replication_probe "${primary_host}" "${other_host}" "${probe1}"
log "Replication probe #1 passed"

if [[ "${ENABLE_SWITCHOVER}" == "1" ]]; then
  log "Executing switchover..."
  target_standby="$(echo "${status_json}" | json_get_first_healthy_standby_node)"
  [[ -n "${target_standby}" ]] || die "No HEALTHY STANDBY node found for switchover"

  max_online_wait="${SWITCHOVER_ONLINE_WAIT_SECONDS:-30}"
  interval_online_wait="${SWITCHOVER_ONLINE_CHECK_INTERVAL:-2}"
  waited=0
  while (( waited < max_online_wait )); do
    status_before_switch="$(get_cluster_status)"
    standby_state="$(echo "${status_before_switch}" | json_get_node_service_state "${target_standby}")"
    if [[ "${standby_state}" == "ONLINE" ]]; then
      break
    fi
    log "Waiting for ${target_standby} to become ONLINE (current=${standby_state:-UNKNOWN})"
    sleep "${interval_online_wait}"
    waited=$((waited + interval_online_wait))
  done
  (( waited < max_online_wait )) || die "Target standby ${target_standby} did not become ONLINE within ${max_online_wait}s"

  curl -fsS -X POST \
    "${AGENT_URL}/cluster/switchover?targetNodeId=${target_standby}" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" >/dev/null

  sleep "${SWITCHOVER_WAIT_SECONDS}"

  status_after="$(get_cluster_status)"
  new_primary="$(echo "${status_after}" | json_get_primary_node)"
  [[ "${new_primary}" == "${target_standby}" ]] || die "Switchover failed: expected ${target_standby}, got ${new_primary}"

  new_primary_host="$(echo "${status_after}" | json_get_host_by_node_id "${new_primary}")"
  other_after_node="$(echo "${status_after}" | json_get_first_healthy_standby_node)"
  other_after_host="$(echo "${status_after}" | json_get_host_by_node_id "${other_after_node}")"
  [[ -n "${new_primary_host}" && -n "${other_after_host}" ]] || die "Failed to resolve hosts after switchover"

  probe2="ha_probe_after_switchover_$(date +%s)"
  run_replication_probe "${new_primary_host}" "${other_after_host}" "${probe2}"
  log "Switchover + replication probe #2 passed"
fi

log "Cleaning up probe nodes..."
cleanup_hosts=("${primary_host}" "${other_host}")
if [[ "${ENABLE_SWITCHOVER}" == "1" && -n "${new_primary_host:-}" && -n "${other_after_host:-}" ]]; then
  cleanup_hosts+=("${new_primary_host}" "${other_after_host}")
fi
# Deduplicate hosts
declare -A seen_hosts
for host in "${cleanup_hosts[@]}"; do
  if [[ -z "${seen_hosts[$host]:-}" ]]; then
    seen_hosts[$host]=1
    docker exec "${host}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
      "MATCH (n:HAProbe) DETACH DELETE n" >/dev/null 2>&1 || true
  fi
done

log "All HA smoke checks passed."
