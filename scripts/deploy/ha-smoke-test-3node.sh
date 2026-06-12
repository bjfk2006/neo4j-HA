#!/usr/bin/env bash
set -euo pipefail

# One-click HA smoke test for 3-node cluster (1 primary + 2 standby).
# Validates:
# 1) Agent health and metrics endpoint
# 2) Cluster state has one primary and at least two standbys
# 3) CDC -> Redis -> SyncApplier replication to all standbys
# 4) Optional switchover and post-switchover replication to all non-primary nodes

AGENT_URL="${AGENT_URL:-http://localhost:8080}"
COMPOSE_FILE="${COMPOSE_FILE:-docker/neo4j/test-compose.yml}"
WAIT_SECONDS="${WAIT_SECONDS:-10}"
SWITCHOVER_WAIT_SECONDS="${SWITCHOVER_WAIT_SECONDS:-15}"
ENABLE_SWITCHOVER="${ENABLE_SWITCHOVER:-1}"
SWITCHOVER_ONLINE_WAIT_SECONDS="${SWITCHOVER_ONLINE_WAIT_SECONDS:-45}"
SWITCHOVER_ONLINE_CHECK_INTERVAL="${SWITCHOVER_ONLINE_CHECK_INTERVAL:-2}"

NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

# Auto-load docker/.env when required credentials are missing.
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
  python3 -c 'import json,sys; print(json.loads(sys.stdin.read() or "{}").get("primaryNode",""))'
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

json_get_standby_nodes() {
  python3 -c 'import json,sys
doc=json.loads(sys.stdin.read() or "{}")
for n in doc.get("nodes",[]):
    if n.get("role")=="STANDBY":
        print(n.get("id",""))'
}

json_get_nodes_except_primary() {
  local primary="$1"
  python3 -c 'import json,sys
doc=json.loads(sys.stdin.read() or "{}")
primary=sys.argv[1]
for n in doc.get("nodes",[]):
    if n.get("id") != primary:
        print(n.get("id",""))' "$primary"
}

json_get_first_online_standby() {
  python3 -c 'import json,sys
doc=json.loads(sys.stdin.read() or "{}")
for n in doc.get("nodes",[]):
    if n.get("role")=="STANDBY" and n.get("serviceState")=="ONLINE":
        print(n.get("id",""))
        break'
}

assert_cluster_healthy_3node() {
  local status_json="$1"
  python3 -c 'import json,sys
doc=json.loads(sys.stdin.read() or "{}")
nodes=doc.get("nodes",[])
if len(nodes) < 3:
    raise SystemExit(f"expected >=3 nodes, got {len(nodes)}")
if not doc.get("primaryNode"):
    raise SystemExit("cluster/status primaryNode is empty")
bad=[n for n in nodes if n.get("health")!="HEALTHY"]
if bad:
    raise SystemExit("unhealthy nodes: " + ", ".join(n.get("id","?") for n in bad))
standbys=[n for n in nodes if n.get("role")=="STANDBY"]
if len(standbys) < 2:
    raise SystemExit(f"expected >=2 standbys, got {len(standbys)}")
print("OK")' <<<"${status_json}"
}

run_replication_probe() {
  local write_host="$1"
  local read_host="$2"
  local probe_id="$3"
  local max_retries="${PROBE_READ_RETRIES:-6}"
  local retry_interval="${PROBE_RETRY_INTERVAL:-5}"

  log "Writing probe '${probe_id}' to ${write_host}"
  docker exec "${write_host}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
    "MERGE (n:HAProbe {id:'${probe_id}'}) SET n.ts=timestamp(), n.msg='ha-smoke-3node' RETURN n.id" >/dev/null

  sleep "${WAIT_SECONDS}"

  local attempt=1
  while (( attempt <= max_retries )); do
    log "Reading probe '${probe_id}' from ${read_host} (attempt ${attempt}/${max_retries})"
    local out
    out="$(docker exec "${read_host}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
      "MATCH (n:HAProbe {id:'${probe_id}'}) RETURN n.id" 2>/dev/null || true)"
    if [[ "${out}" == *"${probe_id}"* ]]; then
      return 0
    fi
    if (( attempt < max_retries )); then
      log "Probe not yet replicated to ${read_host}, retrying in ${retry_interval}s..."
      sleep "${retry_interval}"
    fi
    (( attempt++ ))
  done
  die "Replication probe '${probe_id}' not found on ${read_host} after ${max_retries} attempts"
}

wait_node_online() {
  local node_id="$1"
  local waited=0
  while (( waited < SWITCHOVER_ONLINE_WAIT_SECONDS )); do
    status_now="$(get_cluster_status)"
    state_now="$(echo "${status_now}" | json_get_node_service_state "${node_id}")"
    if [[ "${state_now}" == "ONLINE" ]]; then
      return 0
    fi
    log "Waiting for ${node_id} to become ONLINE (current=${state_now:-UNKNOWN})"
    sleep "${SWITCHOVER_ONLINE_CHECK_INTERVAL}"
    waited=$(( waited + SWITCHOVER_ONLINE_CHECK_INTERVAL ))
  done
  return 1
}

log "Checking docker services status..."
docker compose -f "${COMPOSE_FILE}" ps >/dev/null

log "Checking HA Agent health endpoint..."
health_json="$(curl -fsS "${AGENT_URL}/health")"
[[ "${health_json}" == *"UP"* ]] || die "/health is not UP"

log "Checking metrics endpoint..."
metrics="$(curl -fsS "${AGENT_URL}/metrics")"
[[ "${metrics}" == *"neo4j_ha_sync_lag_ms"* ]] || die "metrics missing neo4j_ha_sync_lag_ms"
[[ "${metrics}" == *"neo4j_ha_cdc_events_published_total"* ]] || die "metrics missing cdc counter"

log "Checking cluster status for 3-node shape..."
status_json="$(get_cluster_status)"
assert_cluster_healthy_3node "${status_json}" >/dev/null

primary_node="$(echo "${status_json}" | json_get_primary_node)"
[[ -n "${primary_node}" ]] || die "primaryNode is empty"
primary_host="$(echo "${status_json}" | json_get_host_by_node_id "${primary_node}")"
[[ -n "${primary_host}" ]] || die "failed to resolve primary host"

mapfile -t standby_nodes < <(echo "${status_json}" | json_get_standby_nodes)
(( ${#standby_nodes[@]} >= 2 )) || die "expected at least 2 standby nodes"

standby_hosts=()
for node in "${standby_nodes[@]}"; do
  host="$(echo "${status_json}" | json_get_host_by_node_id "${node}")"
  [[ -n "${host}" ]] || die "failed to resolve standby host for ${node}"
  standby_hosts+=("${host}")
done

probe1="ha_probe_3node_$(date +%s)"
for host in "${standby_hosts[@]}"; do
  run_replication_probe "${primary_host}" "${host}" "${probe1}"
done
log "Replication probe #1 passed on all standby nodes"

if [[ "${ENABLE_SWITCHOVER}" == "1" ]]; then
  log "Executing switchover..."
  target_standby="$(echo "${status_json}" | json_get_first_online_standby)"
  [[ -n "${target_standby}" ]] || target_standby="${standby_nodes[0]}"

  wait_node_online "${target_standby}" \
    || die "Target standby ${target_standby} did not become ONLINE within ${SWITCHOVER_ONLINE_WAIT_SECONDS}s"

  curl -fsS -X POST \
    "${AGENT_URL}/cluster/switchover?targetNodeId=${target_standby}" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" >/dev/null

  sleep "${SWITCHOVER_WAIT_SECONDS}"

  status_after="$(get_cluster_status)"
  new_primary="$(echo "${status_after}" | json_get_primary_node)"
  [[ "${new_primary}" == "${target_standby}" ]] || die "Switchover failed: expected ${target_standby}, got ${new_primary}"

  new_primary_host="$(echo "${status_after}" | json_get_host_by_node_id "${new_primary}")"
  [[ -n "${new_primary_host}" ]] || die "failed to resolve new primary host"

  mapfile -t post_nodes < <(echo "${status_after}" | json_get_nodes_except_primary "${new_primary}")
  (( ${#post_nodes[@]} >= 2 )) || die "expected >=2 non-primary nodes after switchover"

  probe2="ha_probe_3node_after_switchover_$(date +%s)"
  for node in "${post_nodes[@]}"; do
    host="$(echo "${status_after}" | json_get_host_by_node_id "${node}")"
    [[ -n "${host}" ]] || die "failed to resolve host for node ${node} after switchover"
    run_replication_probe "${new_primary_host}" "${host}" "${probe2}"
  done
  log "Switchover + replication probe #2 passed on all non-primary nodes"
fi

log "Cleaning up probe nodes..."
all_hosts=()
mapfile -t all_nodes_for_cleanup < <(echo "$(get_cluster_status)" | python3 -c 'import json,sys,re
doc=json.loads(sys.stdin.read() or "{}")
seen=set()
for n in doc.get("nodes",[]):
    uri=n.get("boltUri","")
    m=re.match(r"bolt://([^:]+):\d+", uri)
    if m:
        h=m.group(1)
        if h not in seen:
            print(h)
            seen.add(h)')
for host in "${all_nodes_for_cleanup[@]}"; do
  docker exec "${host}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
    "MATCH (n:HAProbe) DETACH DELETE n" >/dev/null 2>&1 || true
done

log "All 3-node HA smoke checks passed."
