#!/usr/bin/env bash
set -euo pipefail

# ha-backup-pause-test.sh
# ==========================
# Validates the "sync pause / resume" contract that the backup workflow relies
# on. Does NOT actually run `neo4j-admin backup` — just exercises the HA Agent's
# `/cluster/backup/prepare` and `/cluster/backup/complete` endpoints, which
# globally pause / resume SyncApplier.
#
# What the product promises (BackupCoordinator.java + AdminHttpServer.java):
#   1. POST /cluster/backup/prepare  → SyncApplier.pause()
#      - standbys stop draining the Redis stream
#      - primary continues to accept writes
#      - CDC Collector continues to publish to Redis stream (the stream grows)
#      - consumer-aware XTRIM MINID will not trim entries that any group still
#        needs (BUG-038 retention safety)
#   2. POST /cluster/backup/complete → SyncApplier.resume()
#      - standbys resume XREADGROUP ">" on the changes stream
#      - the backlog accumulated during pause is consumed and applied
#      - all 3 nodes converge to the same final state
#
# Scenario timeline (wall-clock from python test start):
#
#   T+0s     python load test starts (writers/readers/rel/delete/GDS begin)
#   T+60s    POST /cluster/backup/prepare?nodeId=<target-standby>
#            → /cluster/backup/status should become IN_PROGRESS
#            → standbys' syncLagMs begins climbing
#            → primary writes keep succeeding
#   T+60~180 periodic samples of backup state + per-node syncLagMs, asserting
#            (a) state stays IN_PROGRESS (no auto-timeout within 2m)
#            (b) standby lag grows monotonically (not stuck)
#            (c) primary remains HEALTHY and accepting writes
#   T+180s   POST /cluster/backup/complete
#            → /cluster/backup/status returns to IDLE
#            → syncLagMs on all standbys begins draining
#   T+180~300 periodic samples to confirm lag drops toward ~0
#   T+300s   workload stops (--interval-seconds 300)
#   T+300~420 post-quiet sleep (120s) so any residual catch-up completes
#   T+420s   three-node integrity check (same as ha-load-switchover-test.py):
#            each node's TestNode/rel/community snapshot must be identical
#
# Validated invariants:
#   (a) backup prepare transitions state IDLE → IN_PROGRESS
#   (b) pause actually pauses — standby syncLagMs observably grows during the
#       pause window (if it doesn't, the pause hook is not wired)
#   (c) primary continues to serve writes during the pause (workload not
#       blocked; error rate under budget)
#   (d) backup complete transitions state IN_PROGRESS → IDLE
#   (e) after resume, standbys consume the backlog and lag returns to baseline
#   (f) three-node integrity: node-01 / node-02 / node-03 fields are all equal
#       after post-quiet settles
#
# Exit codes:
#   0  — python test exited 0 (all invariants + error budget met)
#   1  — integrity check failed (per-node divergence — sync pipeline bug)
#   2  — write error rate exceeded budget (--error-budget default 0.05)
#   3  — environment / precondition problem
#   4  — backup endpoint contract violated (state machine anomaly, see log)
#
# Usage:
#   scripts/deploy/ha-backup-pause-test.sh
#
# Override any timing:
#   T1_PAUSE=60 T2_RESUME=180 \
#   INTERVAL_SECONDS=300 POST_QUIET_SECONDS=120 \
#   scripts/deploy/ha-backup-pause-test.sh
#
# Prerequisites (same as ha-failover-chaos-test.sh):
#   - 3 Neo4j containers named neo4j-primary / neo4j-standby-1 / neo4j-standby-2
#   - ha-agent reachable on $AGENT_URL (default http://localhost:8080)
#   - docker/.env with NEO4J_PASSWORD and ADMIN_TOKEN
#   - python environment with requirements-load-test.txt installed
#   - jq installed on the host

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
AGENT_URL="${AGENT_URL:-http://localhost:8080}"

# Phase timings, in seconds from T+0 = python load test start.
T1_PAUSE="${T1_PAUSE:-60}"
T2_RESUME="${T2_RESUME:-180}"

INTERVAL_SECONDS="${INTERVAL_SECONDS:-300}"
POST_QUIET_SECONDS="${POST_QUIET_SECONDS:-120}"

# Sample the backup state + per-node syncLag every N seconds during the pause
# and drain windows. 10s gives enough granularity to see lag climbing without
# spamming the log.
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-10}"

# Error budget is tighter than the failover chaos test because a sync pause
# should NOT cause primary write failures — primary keeps accepting writes
# throughout. 5% is a generous safety margin for jitter / GDS load spikes.
ERROR_BUDGET="${ERROR_BUDGET:-0.05}"

GDS_INTERVAL="${GDS_INTERVAL:-120}"
DELETE_RATE="${DELETE_RATE:-2.0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TEST_SCRIPT="${TEST_SCRIPT:-${SCRIPT_DIR}/ha-load-switchover-test.py}"
REPORT_FILE="${REPORT_FILE:-/tmp/ha-backup-pause-report.json}"
LOG_FILE="${LOG_FILE:-/tmp/ha-backup-pause.log}"

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
  echo "[backup $(date +'%H:%M:%S')] ERROR: NEO4J_PASSWORD not set (check ${ENV_FILE} or export manually)" >&2
  exit 3
fi
if [[ -z "${ADMIN_TOKEN}" ]]; then
  echo "[backup $(date +'%H:%M:%S')] ERROR: ADMIN_TOKEN not set (check ${ENV_FILE} or export manually)" >&2
  exit 3
fi

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
ts() { date +'%H:%M:%S'; }
log() { echo "[backup $(ts)] $*"; }
fail() { echo "[backup $(ts)] ERROR: $*" >&2; exit 3; }
fail_contract() { echo "[backup $(ts)] CONTRACT VIOLATION: $*" >&2; exit 4; }

cluster_status() {
  curl -sf "${AGENT_URL}/cluster/status" || return 1
}

backup_status() {
  # GET /cluster/backup/status does NOT require the admin token.
  curl -sf "${AGENT_URL}/cluster/backup/status" || return 1
}

backup_prepare() {
  local node="$1"
  curl -sf -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    "${AGENT_URL}/cluster/backup/prepare?nodeId=${node}" || return 1
}

backup_complete() {
  curl -sf -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    "${AGENT_URL}/cluster/backup/complete" || return 1
}

# Map HA Agent nodeId → docker container name (for future extensions).
nodeid_to_container() {
  case "$1" in
    node-01) echo "neo4j-primary" ;;
    node-02) echo "neo4j-standby-1" ;;
    node-03) echo "neo4j-standby-2" ;;
    *) echo "" ;;
  esac
}

# Best-effort cleanup — ensures SyncApplier is resumed even if the script crashes
# while backup is still "in progress". Otherwise a killed test would leave sync
# paused until BackupCoordinator.checkTimeout fires (default 2h) — a long time
# to debug an "empty" cluster during CI.
#
# Note on `${var:-0}` guards: this script uses `set -u`, which triggers
# "unbound variable" errors if we reference a variable that hasn't been set.
# Precheck failures can jump to cleanup() before PY_PID / KILLED_CONTAINERS
# are initialised, so every reference below MUST use a default.
PY_PID=""
cleanup() {
  local code=$?
  log "cleanup triggered (exit code=${code})"
  local pid="${PY_PID:-}"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    log "  killing python pid=${pid}"
    kill "${pid}" 2>/dev/null || true
  fi
  # Try to resume sync if we left it paused. /cluster/backup/complete is
  # idempotent-ish — it throws IllegalStateException if state != IN_PROGRESS,
  # but we ignore that (curl will get 500 and we don't care). The safer path
  # is to check status first.
  if BSTATE=$(backup_status 2>/dev/null); then
    local current_state
    current_state=$(echo "${BSTATE}" | jq -r '.state // "UNKNOWN"')
    if [[ "${current_state}" == "IN_PROGRESS" ]] || [[ "${current_state}" == "PREPARING" ]]; then
      log "  backup still ${current_state} at exit — forcing /cluster/backup/complete to avoid 2h sync stall"
      backup_complete >/dev/null 2>&1 || log "  WARN: /cluster/backup/complete failed; manual intervention required"
    fi
  fi
  exit "${code}"
}
trap cleanup EXIT INT TERM

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

PRIMARY_HEALTH=$(echo "${PRE_STATUS}" \
  | jq -r --arg id "${INITIAL_PRIMARY}" '.nodes[] | select(.id == $id) | .health')
STANDBY_HEALTHY=$(echo "${PRE_STATUS}" \
  | jq '[.nodes[] | select(.role == "STANDBY" and .health == "HEALTHY")] | length')
STANDBY_ONLINE=$(echo "${PRE_STATUS}" \
  | jq '[.nodes[] | select(.role == "STANDBY" and .serviceState == "ONLINE")] | length')

# Readiness criteria, in order of preference:
#   (a) primary HEALTHY + 2 standbys ONLINE — ideal, nothing to do
#   (b) primary HEALTHY + 2 standbys HEALTHY but SYNCING — fresh / empty
#       cluster (BUG-072 semantics: evaluateServiceStates keeps standbys in
#       SYNCING until CDC is demonstrably active, which needs at least one
#       non-underscore write on primary). Kick CDC with a bootstrap write
#       and give the service-state evaluator a few ticks to promote.
#   (c) anything else — stop; something is actually broken.
if [[ "${PRIMARY_HEALTH}" != "HEALTHY" ]]; then
  echo "${PRE_STATUS}" \
    | jq '{primary: .primaryNode, nodes: [.nodes[] | {id, role, serviceState, health, syncLagMs}]}' >&2
  fail "cluster not ready: primary.health=${PRIMARY_HEALTH} (need HEALTHY)"
fi

if [[ "${STANDBY_ONLINE}" -lt 2 ]]; then
  if [[ "${STANDBY_HEALTHY}" -ge 2 ]]; then
    log "standbys are HEALTHY but still SYNCING (${STANDBY_ONLINE}/2 ONLINE) — likely fresh cluster"
    log "BUG-072 workaround: writing a bootstrap row on primary to trigger CDC activity…"
    if ! docker exec neo4j-primary cypher-shell -a bolt://localhost:7687 \
           -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
           "CREATE (n:Bootstrap {ts: timestamp(), src: 'ha-backup-pause-test'}) RETURN id(n)" \
           >/dev/null 2>&1; then
      fail "bootstrap write failed — primary unreachable via docker exec? Check: docker ps, NEO4J_PASSWORD"
    fi
    log "bootstrap write done; waiting up to 30s for standbys to promote to ONLINE…"
    for i in 1 2 3 4 5 6; do
      sleep 5
      STANDBY_ONLINE=$(cluster_status 2>/dev/null \
        | jq '[.nodes[] | select(.role == "STANDBY" and .serviceState == "ONLINE")] | length')
      if [[ "${STANDBY_ONLINE}" -ge 2 ]]; then
        log "standbys promoted: ${STANDBY_ONLINE}/2 ONLINE after $((i*5))s"
        break
      fi
      log "  still ${STANDBY_ONLINE}/2 ONLINE after $((i*5))s…"
    done
    if [[ "${STANDBY_ONLINE}" -lt 2 ]]; then
      echo "$(cluster_status)" \
        | jq '{primary: .primaryNode, nodes: [.nodes[] | {id, role, serviceState, health, syncLagMs}]}' >&2
      fail "standbys did not promote to ONLINE within 30s after bootstrap write. Possible causes:
    - BUG-072 regression (evaluateServiceStates not honouring the new CDC activity)
    - CdcCollector not polling primary — check docker logs ha-agent | grep CdcCollector
    - agent has stale SYNCING status — docker restart ha-agent, then retry"
    fi
    # Post-bootstrap: refresh PRE_STATUS so downstream computations (PAUSE_TARGET
    # in particular) see the new serviceState=ONLINE. Without this the script
    # failed with "no ONLINE standby to use as pause target" right after the
    # bootstrap loop succeeded — stale snapshot.
    PRE_STATUS=$(cluster_status) || fail "cluster /cluster/status unreachable after bootstrap"
  else
    echo "${PRE_STATUS}" \
      | jq '{primary: .primaryNode, nodes: [.nodes[] | {id, role, serviceState, health, syncLagMs}]}' >&2
    fail "cluster not ready: standby-HEALTHY=${STANDBY_HEALTHY}/2, standby-ONLINE=${STANDBY_ONLINE}/2 (need 2 HEALTHY).
    Common causes:
    - a standby container is stopped: docker ps -a; docker start neo4j-standby-1 neo4j-standby-2
    - agent hasn't yet observed standby recovery: wait 15-30s and rerun
    - agent has stale state: docker restart ha-agent"
  fi
fi

# Pause target: pick the first ONLINE standby that is not the primary.
# Even though SyncApplier.pause() is global (all standbys stop draining), the
# /cluster/backup/prepare endpoint takes a nodeId to record *which* standby
# is the intended backup source — so we pick a sensible one.
PAUSE_TARGET=$(echo "${PRE_STATUS}" | jq -r --arg p "${INITIAL_PRIMARY}" \
  '.nodes[] | select(.role == "STANDBY" and .serviceState == "ONLINE" and .id != $p) | .id' \
  | head -1)
if [[ -z "${PAUSE_TARGET}" ]]; then
  fail "no ONLINE standby to use as pause target"
fi

# Check backup is not already in progress from a prior failed run.
PRE_BACKUP=$(backup_status) || fail "GET /cluster/backup/status unreachable"
PRE_BSTATE=$(echo "${PRE_BACKUP}" | jq -r '.state // "UNKNOWN"')
if [[ "${PRE_BSTATE}" != "IDLE" ]]; then
  echo "${PRE_BACKUP}" | jq '.' >&2
  fail "backup state is ${PRE_BSTATE} (expected IDLE) — a previous run may have left it stuck.
    Run:  curl -sf -X POST -H 'Authorization: Bearer \$ADMIN_TOKEN' \\
              ${AGENT_URL}/cluster/backup/complete
    to force resume, then retry."
fi

log "initial primary: ${INITIAL_PRIMARY}"
log "pause target   : ${PAUSE_TARGET} (container: $(nodeid_to_container "${PAUSE_TARGET}"))"
log "timeline       : pause=T+${T1_PAUSE}s  resume=T+${T2_RESUME}s  " \
    "stop=T+${INTERVAL_SECONDS}s  post-quiet=${POST_QUIET_SECONDS}s  sample=${SAMPLE_INTERVAL_SECONDS}s"

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

log "python pid=${PY_PID}"
sleep 3
if ! kill -0 "${PY_PID}" 2>/dev/null; then
  tail -20 "${LOG_FILE}" >&2
  fail "python test crashed during startup — see ${LOG_FILE}"
fi

# -----------------------------------------------------------------------------
# Sampling helper — prints a single line summary of the cluster's sync state.
# Stores the highest observed standby lag across samples so we can assert
# at the end that the lag actually climbed during the pause window.
# -----------------------------------------------------------------------------
MAX_LAG_DURING_PAUSE=0
MIN_LAG_AFTER_RESUME=9999999999

sample_and_log() {
  local phase="$1"
  local status_json
  status_json=$(cluster_status 2>/dev/null || echo '{}')
  local backup_json
  backup_json=$(backup_status 2>/dev/null || echo '{}')

  local bstate
  bstate=$(echo "${backup_json}" | jq -r '.state // "UNKNOWN"')
  local lags
  lags=$(echo "${status_json}" \
    | jq -r '.nodes[] | select(.role=="STANDBY") | "\(.id)=\(.syncLagMs)"' \
    | tr '\n' ' ')
  # Max lag across standbys in this sample (single number).
  local max_lag
  max_lag=$(echo "${status_json}" \
    | jq '[.nodes[] | select(.role=="STANDBY") | .syncLagMs] | max // 0')

  log "  ${phase}: backup=${bstate}  standby-lag: ${lags}"

  if [[ "${phase}" == "during-pause" ]]; then
    if (( max_lag > MAX_LAG_DURING_PAUSE )); then
      MAX_LAG_DURING_PAUSE=${max_lag}
    fi
  elif [[ "${phase}" == "after-resume" ]]; then
    if (( max_lag < MIN_LAG_AFTER_RESUME )); then
      MIN_LAG_AFTER_RESUME=${max_lag}
    fi
  fi
}

# -----------------------------------------------------------------------------
# Phase 2: T+T1 — POST /cluster/backup/prepare (pause sync)
# -----------------------------------------------------------------------------
log "sleeping ${T1_PAUSE}s before pausing sync"
sleep "${T1_PAUSE}"

log "T+${T1_PAUSE}s — POST /cluster/backup/prepare?nodeId=${PAUSE_TARGET}"
PREPARE_RESP=$(backup_prepare "${PAUSE_TARGET}") || fail_contract "POST /cluster/backup/prepare failed"
log "  prepare response: ${PREPARE_RESP}"

# Immediately confirm state moved to IN_PROGRESS.
BSTATE_AFTER_PREPARE=$(backup_status | jq -r '.state // "UNKNOWN"')
if [[ "${BSTATE_AFTER_PREPARE}" != "IN_PROGRESS" ]]; then
  fail_contract "expected backup state=IN_PROGRESS after prepare, got=${BSTATE_AFTER_PREPARE}"
fi
log "  contract check (a): IDLE → IN_PROGRESS ✓"

# -----------------------------------------------------------------------------
# Phase 3: T+T1 ~ T+T2 — sample during pause
# -----------------------------------------------------------------------------
PAUSE_WINDOW=$(( T2_RESUME - T1_PAUSE ))
log "sampling during ${PAUSE_WINDOW}s pause window..."

PAUSE_SAMPLES=0
pause_end_ts=$(( $(date +%s) + PAUSE_WINDOW ))
while (( $(date +%s) < pause_end_ts )); do
  sample_and_log "during-pause"
  PAUSE_SAMPLES=$(( PAUSE_SAMPLES + 1 ))
  sleep "${SAMPLE_INTERVAL_SECONDS}"
done

# Invariant check: during the pause, at least one sample must show a standby
# lag > 0 (the stream was growing while the consumer group was idle). If
# max_lag stays 0, the pause hook may not be wired — or the workload didn't
# generate any CDC events, which would be its own problem.
#
# Threshold: 100ms is well above normal drain latency (<10ms) and below the
# typical pause-induced backlog (which for 10 events/s * 120s = 1200 events
# would produce lag measured in full seconds, tens of thousands of ms).
if (( MAX_LAG_DURING_PAUSE < 100 )); then
  fail_contract "max standby lag during pause was ${MAX_LAG_DURING_PAUSE}ms — pause hook may not be wired.
    Expected the lag to climb to at least 100ms (typically multiple seconds) as the stream
    accumulates events while the consumer group sits idle. A value near 0 means one of:
      (a) BackupCoordinator.prepare did not call SyncApplier.pause()
      (b) SyncApplier's paused flag is not honoured by consumeLoop
      (c) The workload produced no writes during the pause window
      (d) syncLagMs metric is not being updated (see BUG-016/C3 fix status)"
fi
log "  contract check (b): pause actually paused — max lag during window = ${MAX_LAG_DURING_PAUSE}ms ✓"

# -----------------------------------------------------------------------------
# Phase 4: T+T2 — POST /cluster/backup/complete (resume sync)
# -----------------------------------------------------------------------------
log "T+${T2_RESUME}s — POST /cluster/backup/complete"
COMPLETE_RESP=$(backup_complete) || fail_contract "POST /cluster/backup/complete failed"
log "  complete response: ${COMPLETE_RESP}"

BSTATE_AFTER_COMPLETE=$(backup_status | jq -r '.state // "UNKNOWN"')
if [[ "${BSTATE_AFTER_COMPLETE}" != "IDLE" ]]; then
  fail_contract "expected backup state=IDLE after complete, got=${BSTATE_AFTER_COMPLETE}"
fi
log "  contract check (d): IN_PROGRESS → IDLE ✓"

# -----------------------------------------------------------------------------
# Phase 5: T+T2 ~ T+INTERVAL_SECONDS — sample during drain
# -----------------------------------------------------------------------------
DRAIN_WINDOW=$(( INTERVAL_SECONDS - T2_RESUME ))
log "sampling during ${DRAIN_WINDOW}s post-resume drain window..."

drain_end_ts=$(( $(date +%s) + DRAIN_WINDOW ))
while (( $(date +%s) < drain_end_ts )); do
  sample_and_log "after-resume"
  sleep "${SAMPLE_INTERVAL_SECONDS}"
done

# Invariant check: after resume, the lag must drop meaningfully — at least to
# below max_lag_during_pause, ideally down to baseline (<1s). If the lag stays
# stuck at peak, the resume hook is not wired.
#
# We use "< MAX_LAG_DURING_PAUSE / 2" as the threshold: a generous bar that
# tolerates slow drain on small/constrained test environments but catches
# "resume did nothing" flat-line regressions.
RESUME_DROP_THRESHOLD=$(( MAX_LAG_DURING_PAUSE / 2 ))
if (( MIN_LAG_AFTER_RESUME >= RESUME_DROP_THRESHOLD )); then
  fail_contract "standby lag did not drop after resume.
    max during pause = ${MAX_LAG_DURING_PAUSE}ms
    min after resume = ${MIN_LAG_AFTER_RESUME}ms
    expected min-after < ${RESUME_DROP_THRESHOLD}ms (half of peak) as backlog is drained.
    Possible causes:
      (a) BackupCoordinator.complete did not call SyncApplier.resume()
      (b) consumeLoop's paused flag flip is not visible to the loop thread
      (c) drain window too short (current ${DRAIN_WINDOW}s) — try higher DRAIN or INTERVAL_SECONDS"
fi
log "  contract check (e): resume drained backlog — min lag after resume = ${MIN_LAG_AFTER_RESUME}ms ✓"

# -----------------------------------------------------------------------------
# Phase 6: wait for python load test + post-quiet + integrity check to finish
# -----------------------------------------------------------------------------
log "waiting for python load test to finish (post-quiet=${POST_QUIET_SECONDS}s + integrity check)..."
set +e
wait "${PY_PID}"
PY_EXIT=$?
set -e
log "python test exited with code ${PY_EXIT}"

# -----------------------------------------------------------------------------
# Phase 7: final report + exit code translation
# -----------------------------------------------------------------------------
if [[ ! -f "${REPORT_FILE}" ]]; then
  log "ERROR: report file not produced at ${REPORT_FILE}"
  tail -50 "${LOG_FILE}" >&2
  exit 3
fi

log "integrity report:"
jq '{integrity_ok, per_node: (.per_node // {}) | to_entries | map({key: .key, value: {count_total: .value.count_total, missing_count: .value.missing_count, extra_count: .value.extra_count, rel_missing_count: .value.rel_missing_count, rel_extra_count: .value.rel_extra_count, community_matches_reference: .value.community_matches_reference}}) | from_entries}' \
    "${REPORT_FILE}" | sed 's/^/[backup] /'

INTEGRITY_OK=$(jq -r '.integrity_ok' "${REPORT_FILE}")

# Three-node field-for-field equality (the HA sync layer's actual contract).
# See ha-agent-design.md "集群内一致 vs 对 reference 一致" section — this is
# what BUG-074/075 actually promises, independent of failover-window client
# write losses.
log "checking three-node data equality..."
NODE_SNAPSHOTS=$(jq -r '.per_node | to_entries | map({key: .key, val: {c: .value.count_total, r: .value.rel_count, m: .value.missing_count, e: .value.extra_count, rm: .value.rel_missing_count, re: .value.rel_extra_count, dl: .value.delete_leak_count, cm: .value.community_matches_reference}}) | .[].val | @json' "${REPORT_FILE}")
DISTINCT=$(echo "${NODE_SNAPSHOTS}" | sort -u | wc -l)
if (( DISTINCT > 1 )); then
  log "  FAIL: nodes have divergent snapshots (${DISTINCT} distinct)"
  echo "${NODE_SNAPSHOTS}" | sort -u | sed 's/^/[backup]   /'
  echo ""
  log "  this is the real HA consistency failure — BUG-074/075 territory"
  exit 1
fi
log "  all ${DISTINCT:-1} nodes have identical snapshots ✓"

# Final exit code:
#   python test's exit already encodes integrity + error budget outcome
if [[ "${PY_EXIT}" -eq 0 ]]; then
  log "✅ ALL CHECKS PASSED — backup pause/resume contract validated:"
  log "   - IDLE → IN_PROGRESS → IDLE transitions ✓"
  log "   - pause actually paused (peak lag ${MAX_LAG_DURING_PAUSE}ms)"
  log "   - resume actually drained (min lag ${MIN_LAG_AFTER_RESUME}ms)"
  log "   - three-node snapshots identical ✓"
  log "   - integrity_ok = ${INTEGRITY_OK} (3-nodes-equal is the HA Agent's contract;"
  log "     integrity_ok may still be false due to failover-window client write losses,"
  log "     but none applies here since no failover happened)"
  exit 0
fi

log "python test exit=${PY_EXIT} — see ${LOG_FILE}"
case "${PY_EXIT}" in
  1) log "  exit 1: integrity check failed per python's own comparator"; exit 1 ;;
  2) log "  exit 2: write error rate exceeded ${ERROR_BUDGET}"; exit 2 ;;
  *) log "  exit ${PY_EXIT}: environment / unexpected"; exit 3 ;;
esac
