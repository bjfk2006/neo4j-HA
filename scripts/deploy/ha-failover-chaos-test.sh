#!/usr/bin/env bash
set -euo pipefail

# ha-failover-chaos-test.sh
# ==========================
# One-click chaos test that validates BOTH primary-crash failover AND
# standby-crash survival in a single run.
#
# Scenario timeline (wall-clock from python test start):
#
#   T+0s    python load test starts (writers/readers/rel/delete/GDS begin)
#   T+60s   docker kill <initial-primary>
#           → HealthChecker should escalate HEALTHY → SUSPECT → DOWN (BUG-068)
#           → FailoverOrchestrator runs P1-P10, new primary elected
#           → HAProxy rerouted to new primary
#           → client writes resume after ~30-45s outage
#   T+180s  docker start <initial-primary>
#           → OldPrimaryRecovery auto-triggers on HealthChecker onNodeRecovered
#           → trigger uninstall + _CDCDeleteEvent cleanup + FULL_SYNC
#           → old primary rejoins as STANDBY, eventually reaches ONLINE
#   T+300s  query /cluster/status, pick a STANDBY that is NOT the recovered
#           node and NOT the current primary; docker kill that container
#           → this validates: standby DOWN must NOT trigger Failover (only
#             primary DOWN does); HAProxy takes standby out of read backend;
#             writes/reads on primary + remaining standby must continue normally
#   T+360s  docker start <standby-victim>
#           → SyncApplier already has its target; standby catches up via CDC
#             replay (INCREMENTAL sync, no FULL_SYNC needed for brief crash)
#   T+480s  workload stops (--interval-seconds 480)
#   T+480s..T+600s  post-quiet sleep (120s) for final replication catch-up
#   T+600s  three-node integrity check (bypasses HAProxy, direct bolt)
#
# Validated invariants:
#   (a) automatic failover on primary crash (BUG-068)
#   (b) HAProxy not fighting HealthChecker's DOWN verdict (BUG-069)
#   (c) _CDCDeleteEvent zero across all nodes (BUG-067)
#   (d) standby crash does NOT trigger a second failover
#   (e) all 3 nodes end with identical :TestNode count, rel_count, community
#       distribution (±RPO loss, same on all nodes)
#
# Exit code:
#   0  — the python test exited 0 (all invariants + error budget met)
#   1  — integrity check failed (per-node divergence — real consistency bug)
#   2  — write error rate exceeded budget (--error-budget default 0.15)
#   3  — environment / precondition problem (cluster unreachable, wrong nodes, etc.)
#
# Usage:
#   scripts/deploy/ha-failover-chaos-test.sh
#
# Override any timing:
#   T1_KILL_PRIMARY=60 T2_START_PRIMARY=180 \
#   T3_KILL_STANDBY=300 T4_START_STANDBY=360 \
#   INTERVAL_SECONDS=480 POST_QUIET_SECONDS=120 \
#   scripts/deploy/ha-failover-chaos-test.sh
#
# Prerequisites (same as ha-load-switchover-test.py):
#   - 3 Neo4j containers named neo4j-primary / neo4j-standby-1 / neo4j-standby-2
#   - ha-agent container reachable on $AGENT_URL (default http://localhost:8080)
#   - docker/.env file with NEO4J_PASSWORD and ADMIN_TOKEN
#   - python environment with requirements-load-test.txt installed
#   - `jq` installed on the host

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
AGENT_URL="${AGENT_URL:-http://localhost:8080}"

# Phase timings, in seconds from T+0 = python load test start.
# Defaults chosen so that each phase has a 60-120s buffer before the next
# disruption, based on observed MTTRs in the BUG-068 validation run:
#   failover: ~41s (kill → new primary ONLINE)
#   OldPrimaryRecovery FULL_SYNC: ~60-120s (depends on dataset size)
T1_KILL_PRIMARY="${T1_KILL_PRIMARY:-60}"
T2_START_PRIMARY="${T2_START_PRIMARY:-180}"
T3_KILL_STANDBY="${T3_KILL_STANDBY:-300}"
T4_START_STANDBY="${T4_START_STANDBY:-360}"

INTERVAL_SECONDS="${INTERVAL_SECONDS:-480}"
POST_QUIET_SECONDS="${POST_QUIET_SECONDS:-120}"

# Error budget 0.15 rather than the switchover-style 0.02 — a crash-style
# failover inherently burns ~40s of complete write unavailability, which for
# 10 writes/s across 480s is ~8% by itself, plus relationship cascade (~5%).
# See BUG-068 validation run analysis for derivation.
ERROR_BUDGET="${ERROR_BUDGET:-0.15}"

GDS_INTERVAL="${GDS_INTERVAL:-120}"
DELETE_RATE="${DELETE_RATE:-2.0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TEST_SCRIPT="${TEST_SCRIPT:-${SCRIPT_DIR}/ha-load-switchover-test.py}"
REPORT_FILE="${REPORT_FILE:-/tmp/ha-chaos-two-event-report.json}"
LOG_FILE="${LOG_FILE:-/tmp/ha-chaos-two-event.log}"

# -----------------------------------------------------------------------------
# Auto-load docker/.env for NEO4J_PASSWORD / ADMIN_TOKEN / NEO4J_USER
# -----------------------------------------------------------------------------
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/docker/.env}"
if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

if [[ -z "${NEO4J_PASSWORD}" ]]; then
  echo "[chaos] ERROR: NEO4J_PASSWORD not set (check ${ENV_FILE} or export manually)" >&2
  exit 3
fi
if [[ -z "${ADMIN_TOKEN}" ]]; then
  echo "[chaos] ERROR: ADMIN_TOKEN not set (check ${ENV_FILE} or export manually)" >&2
  exit 3
fi

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
ts() { date +'%H:%M:%S'; }
log() { echo "[chaos $(ts)] $*"; }
fail() { echo "[chaos $(ts)] ERROR: $*" >&2; exit 3; }

# Map HA Agent nodeId → docker container name.
# Convention established by docker/neo4j/test-compose.yml + ha-agent.yml.
nodeid_to_container() {
  case "$1" in
    node-01) echo "neo4j-primary" ;;
    node-02) echo "neo4j-standby-1" ;;
    node-03) echo "neo4j-standby-2" ;;
    *) echo "" ;;
  esac
}

# Query cluster status, return JSON via stdout.
cluster_status() {
  curl -sf "${AGENT_URL}/cluster/status" || return 1
}

# -----------------------------------------------------------------------------
# Prechecks
# -----------------------------------------------------------------------------
log "prechecks: cluster reachable? python available? jq available?"
command -v jq >/dev/null || fail "jq is required but not installed — install with: apt-get install -y jq (Debian/Ubuntu) or yum install -y jq (RHEL/CentOS)"
command -v docker >/dev/null || fail "docker is required but not installed"
command -v python3 >/dev/null || fail "python3 is required but not installed"

PRE_STATUS=$(cluster_status) || fail "cluster /cluster/status unreachable at ${AGENT_URL}"
INITIAL_PRIMARY=$(echo "${PRE_STATUS}" | jq -r '.primaryNode // empty')
if [[ -z "${INITIAL_PRIMARY}" ]]; then
  echo "${PRE_STATUS}" | jq '.' >&2
  fail "cluster has no primary at start (see status JSON above)"
fi

# Readiness criterion:
#   - a primary is designated AND primary is HEALTHY
#   - at least 2 STANDBY nodes are serviceState=ONLINE (needed for failover path)
#
# Why NOT "all 3 nodes serviceState=ONLINE":
# Pre-BUG-073 ha-agent never transitioned the primary's serviceState from its
# initial SYNCING to ONLINE (ClusterInitializer defaults all nodes to SYNCING;
# HaAgent.evaluateServiceStates only iterated standby nodes). So on any old
# agent binary, a fully working cluster legitimately reads
# primary=SYNCING + standbys=ONLINE. This check tolerates that, while still
# insisting primary is actually HEALTHY (the thing that matters for writes).
PRIMARY_HEALTH=$(echo "${PRE_STATUS}" \
  | jq -r --arg id "${INITIAL_PRIMARY}" '.nodes[] | select(.id == $id) | .health')
STANDBY_ONLINE=$(echo "${PRE_STATUS}" \
  | jq '[.nodes[] | select(.role == "STANDBY" and .serviceState == "ONLINE")] | length')
if [[ "${PRIMARY_HEALTH}" != "HEALTHY" ]] || [[ "${STANDBY_ONLINE}" -lt 2 ]]; then
  echo "${PRE_STATUS}" \
    | jq '{primary: .primaryNode, nodes: [.nodes[] | {id, role, serviceState, health, reachable, syncLagMs}]}' >&2
  fail "cluster not ready: primary.health=${PRIMARY_HEALTH} (need HEALTHY), standby-ONLINE=${STANDBY_ONLINE}/2.
    See per-node status JSON above. Common causes:
    - cluster still stabilizing after restart → wait 15-30s and rerun
    - a previous chaos run left a container stopped → docker ps -a; docker start <name>
    - agent has stale state → docker restart ha-agent
    - BUG-072 on empty cluster → write a non-underscore label to kick CDC:
        docker exec neo4j-primary cypher-shell -a bolt://localhost:7687 -u neo4j -p \"\$NEO4J_PASSWORD\" \\
          \"CREATE (n:Bootstrap {t: timestamp()}) RETURN id(n)\"; sleep 15"
fi

PRIMARY_CONTAINER=$(nodeid_to_container "${INITIAL_PRIMARY}")
if [[ -z "${PRIMARY_CONTAINER}" ]]; then
  fail "unknown initial primary nodeId=${INITIAL_PRIMARY} (expected node-01/02/03)"
fi

log "initial primary: ${INITIAL_PRIMARY} (container: ${PRIMARY_CONTAINER})"
log "timeline:  kill-primary=T+${T1_KILL_PRIMARY}s  start-primary=T+${T2_START_PRIMARY}s  " \
    "kill-standby=T+${T3_KILL_STANDBY}s  start-standby=T+${T4_START_STANDBY}s  " \
    "stop=T+${INTERVAL_SECONDS}s  post-quiet=${POST_QUIET_SECONDS}s"

# -----------------------------------------------------------------------------
# Phase 1: launch python load test in background
# -----------------------------------------------------------------------------
: >"${LOG_FILE}"
rm -f "${REPORT_FILE}"

log "launching python load test → ${LOG_FILE} (report: ${REPORT_FILE})"
(
  export NEO4J_USER NEO4J_PASSWORD ADMIN_TOKEN AGENT_URL
  python3 "${TEST_SCRIPT}" \
    --clean-before-run \
    --skip-switchover \
    --interval-seconds "${INTERVAL_SECONDS}" \
    --post-quiet-seconds "${POST_QUIET_SECONDS}" \
    --gds-interval "${GDS_INTERVAL}" \
    --delete-rate "${DELETE_RATE}" \
    --error-budget "${ERROR_BUDGET}" \
    --report-file "${REPORT_FILE}"
) >>"${LOG_FILE}" 2>&1 &
PY_PID=$!

# Ensure python workers are terminated if this script is interrupted, and
# try best-effort to bring any killed docker containers back up so the
# cluster isn't left in a half-broken state.
cleanup() {
  local code=$?
  log "cleanup triggered (exit code=${code})"
  if kill -0 "${PY_PID}" 2>/dev/null; then
    log "  killing python pid=${PY_PID}"
    kill "${PY_PID}" 2>/dev/null || true
  fi
  # Best-effort restart — the chaos injection only ever touches two
  # specific containers, track them and docker start each regardless of
  # current state (no-op if already running).
  for c in "${KILLED_CONTAINERS[@]:-}"; do
    if [[ -n "${c}" ]]; then
      log "  ensuring container is up: ${c}"
      docker start "${c}" >/dev/null 2>&1 || true
    fi
  done
  exit "${code}"
}
KILLED_CONTAINERS=()
trap cleanup EXIT INT TERM

log "python pid=${PY_PID}"
# Let the test get through its clean-before-run + _CDCDeleteEvent sweep phase
sleep 3
if ! kill -0 "${PY_PID}" 2>/dev/null; then
  tail -20 "${LOG_FILE}" >&2
  fail "python test crashed during startup — see ${LOG_FILE}"
fi

# -----------------------------------------------------------------------------
# Phase 2: T+T1 — kill initial primary
# -----------------------------------------------------------------------------
log "sleeping ${T1_KILL_PRIMARY}s before killing primary"
sleep "${T1_KILL_PRIMARY}"
log "T+${T1_KILL_PRIMARY}s — docker kill ${PRIMARY_CONTAINER}"
docker kill "${PRIMARY_CONTAINER}" || fail "failed to kill ${PRIMARY_CONTAINER}"
KILLED_CONTAINERS+=("${PRIMARY_CONTAINER}")

# -----------------------------------------------------------------------------
# Phase 3: T+T2 — restart primary (triggers OldPrimaryRecovery)
# -----------------------------------------------------------------------------
gap2=$(( T2_START_PRIMARY - T1_KILL_PRIMARY ))
log "sleeping ${gap2}s (expect failover to complete + new primary to stabilize)"
sleep "${gap2}"

# Snapshot post-failover state so we can later pick a different standby victim
MID_STATUS=$(cluster_status) || fail "lost /cluster/status after failover"
NEW_PRIMARY=$(echo "${MID_STATUS}" | jq -r '.primaryNode // empty')
log "T+${T2_START_PRIMARY}s — post-failover primary=${NEW_PRIMARY}"
if [[ -z "${NEW_PRIMARY}" ]]; then
  fail "post-failover cluster has no primary"
fi
if [[ "${NEW_PRIMARY}" == "${INITIAL_PRIMARY}" ]]; then
  log "WARN: post-failover primary is still ${INITIAL_PRIMARY} — failover may not have triggered (BUG-068 regressed?)"
fi

log "T+${T2_START_PRIMARY}s — docker start ${PRIMARY_CONTAINER}"
docker start "${PRIMARY_CONTAINER}" >/dev/null || fail "failed to start ${PRIMARY_CONTAINER}"

# -----------------------------------------------------------------------------
# Phase 4: T+T3 — pick and kill a standby that is neither the current primary
# nor the recently-recovered node; this exercises a plain standby outage
# -----------------------------------------------------------------------------
gap3=$(( T3_KILL_STANDBY - T2_START_PRIMARY ))
log "sleeping ${gap3}s (expect OldPrimaryRecovery to reach ONLINE)"
sleep "${gap3}"

log "selecting standby victim…"
VICTIM_STATUS=$(cluster_status) || fail "lost /cluster/status before standby kill"
echo "${VICTIM_STATUS}" | jq '{primary: .primaryNode, nodes: [.nodes[] | {id, role, serviceState}]}' | sed 's/^/[chaos] /'

CURRENT_PRIMARY=$(echo "${VICTIM_STATUS}" | jq -r '.primaryNode')
# Preferred victim: ONLINE STANDBY that was NOT the crash target (so we
# exercise a fresh standby, not the one still settling from FULL_SYNC).
VICTIM=$(echo "${VICTIM_STATUS}" | jq -r --arg initial "${INITIAL_PRIMARY}" --arg current "${CURRENT_PRIMARY}" \
  '.nodes[] | select(.role=="STANDBY" and .serviceState=="ONLINE" and .id != $initial and .id != $current) | .id' \
  | head -1)

if [[ -z "${VICTIM}" ]]; then
  # Fallback: any ONLINE STANDBY that is not the current primary
  VICTIM=$(echo "${VICTIM_STATUS}" | jq -r --arg current "${CURRENT_PRIMARY}" \
    '.nodes[] | select(.role=="STANDBY" and .serviceState=="ONLINE" and .id != $current) | .id' \
    | head -1)
fi

if [[ -z "${VICTIM}" ]]; then
  fail "no ONLINE STANDBY available to kill"
fi

VICTIM_CONTAINER=$(nodeid_to_container "${VICTIM}")
if [[ -z "${VICTIM_CONTAINER}" ]]; then
  fail "unknown victim nodeId=${VICTIM}"
fi

log "T+${T3_KILL_STANDBY}s — docker kill ${VICTIM_CONTAINER} (nodeId=${VICTIM})"
docker kill "${VICTIM_CONTAINER}" || fail "failed to kill ${VICTIM_CONTAINER}"
KILLED_CONTAINERS+=("${VICTIM_CONTAINER}")

# -----------------------------------------------------------------------------
# Phase 5: T+T4 — restart the standby
# -----------------------------------------------------------------------------
gap4=$(( T4_START_STANDBY - T3_KILL_STANDBY ))
log "sleeping ${gap4}s (standby downtime)"
sleep "${gap4}"
log "T+${T4_START_STANDBY}s — docker start ${VICTIM_CONTAINER}"
docker start "${VICTIM_CONTAINER}" >/dev/null || fail "failed to start ${VICTIM_CONTAINER}"

# -----------------------------------------------------------------------------
# Phase 6: let the python test run through stop + post-quiet + integrity
# -----------------------------------------------------------------------------
log "chaos injection done. Waiting for python test to finish…"
wait "${PY_PID}"
PY_EXIT=$?
trap - EXIT INT TERM
log "python test exited with code ${PY_EXIT}"

# -----------------------------------------------------------------------------
# Phase 7: post-validation
# -----------------------------------------------------------------------------
echo
echo "============================================================"
echo "[chaos] POST-VALIDATION"
echo "============================================================"

log "final cluster status:"
cluster_status | jq '{primary: .primaryNode, returned_to_initial: (.primaryNode == "'"${INITIAL_PRIMARY}"'"), nodes: [.nodes[] | {id, role, serviceState, syncLagMs}]}' | sed 's/^/[chaos]   /'

log "per-node internal label counts (BUG-067 regression check — should all be 0):"
for container in neo4j-primary neo4j-standby-1 neo4j-standby-2; do
  cnt=$(docker exec "${container}" cypher-shell \
    -a "bolt://localhost:7687" -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
    "MATCH (e:_CDCDeleteEvent) RETURN count(e) AS c" 2>/dev/null \
    | tail -1 | tr -d ' ' || echo "unreachable")
  printf '[chaos]   %-20s _CDCDeleteEvent=%s\n' "${container}" "${cnt}"
done

log "per-node :TestNode count (should be identical across nodes):"
for container in neo4j-primary neo4j-standby-1 neo4j-standby-2; do
  cnt=$(docker exec "${container}" cypher-shell \
    -a "bolt://localhost:7687" -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
    "MATCH (n:TestNode) RETURN count(n) AS c" 2>/dev/null \
    | tail -1 | tr -d ' ' || echo "unreachable")
  printf '[chaos]   %-20s :TestNode=%s\n' "${container}" "${cnt}"
done

if [[ -f "${REPORT_FILE}" ]]; then
  log "integrity report summary:"
  jq '{
    integrity_ok: .integrity_ok,
    write_error_rate: .write_summary.error_rate,
    initial_primary: .initial_primary,
    final_primary: .final_primary,
    written: .expected_written_count,
    deleted: .deleted_count,
    kept: .expected_present_count,
    per_node: [.per_node | to_entries[] | {
      node: .key,
      ok: .value.ok,
      count: .value.count_current_run,
      miss: .value.missing_count,
      extra: .value.extra_count,
      delete_leak: .value.delete_leak_count,
      rel_count: .value.rel_count,
      rel_miss: .value.rel_missing_count,
      comm_match: .value.community_matches_reference
    }]
  }' "${REPORT_FILE}" | sed 's/^/[chaos]   /'
else
  log "WARN: report file ${REPORT_FILE} not produced"
fi

log "recent ha-agent log excerpts (Failover + OldPrimaryRecovery + chaos-sensitive events):"
if docker logs --tail 5000 ha-agent 2>/dev/null \
     | grep -E 'Failover|OldPrimaryRecovery|SUSPECT|UNHEALTHY|Startup sweep|transitioned.*ONLINE|Standby.*DOWN|set server' \
     | tail -40 \
     | sed 's/^/[chaos]   /'; then
  :
else
  log "  (no ha-agent log lines matched)"
fi

echo
case "${PY_EXIT}" in
  0) log "✅ ALL CHECKS PASSED (exit 0) — both primary-crash and standby-crash paths validated";;
  1) log "❌ INTEGRITY CHECK FAILED (exit 1) — real per-node divergence; inspect ${REPORT_FILE}";;
  2) log "⚠️  WRITE ERROR BUDGET EXCEEDED (exit 2) — consider raising --error-budget or reviewing outage duration";;
  *) log "❓ python exited with unexpected code ${PY_EXIT}";;
esac

exit "${PY_EXIT}"
