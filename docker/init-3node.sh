#!/usr/bin/env bash
# Allow invocation via `sh docker/init-3node.sh` by re-execing with bash.
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${PROJECT_ROOT}/docker/neo4j/test-compose.yml}"
ENV_FILE="${PROJECT_ROOT}/docker/.env"
USE_HA_AGENT_BUILD_OVERLAY="${USE_HA_AGENT_BUILD_OVERLAY:-1}"

# Service names can be overridden for different compose naming.
PRIMARY_SERVICE="${PRIMARY_SERVICE:-neo4j-primary}"
STANDBY1_SERVICE="${STANDBY1_SERVICE:-neo4j-standby-1}"
STANDBY2_SERVICE="${STANDBY2_SERVICE:-neo4j-standby-2}"
HAPROXY_SERVICES="${HAPROXY_SERVICES:-haproxy-1 haproxy-2}"
AGENT_SERVICE="${AGENT_SERVICE:-ha-agent}"

# Container names default to service names.
PRIMARY_CONTAINER="${PRIMARY_CONTAINER:-${PRIMARY_SERVICE}}"
STANDBY1_CONTAINER="${STANDBY1_CONTAINER:-${STANDBY1_SERVICE}}"
STANDBY2_CONTAINER="${STANDBY2_CONTAINER:-${STANDBY2_SERVICE}}"

# Data directories can be overridden if your mount layout differs.
NODE1_DIR="${NODE1_DIR:-/opt/neo4j-node1}"
NODE2_DIR="${NODE2_DIR:-/opt/neo4j-node2}"
NODE3_DIR="${NODE3_DIR:-/opt/neo4j-node3}"

# ── Load .env ────────────────────────────────────────────────────────────────
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[ERROR] ${ENV_FILE} not found. Run: cp docker/.env.example docker/.env"
  exit 1
fi
set -a; source "${ENV_FILE}"; set +a

REDIS_HOST="${REDIS_HOST:-172.19.0.11}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
REDIS_CLI_TIMEOUT_SEC="${REDIS_CLI_TIMEOUT_SEC:-5}"

COMPOSE_EXTRA=()
if [[ "${USE_HA_AGENT_BUILD_OVERLAY}" == "1" && -f "${PROJECT_ROOT}/docker/neo4j/test-compose.dev.yml" ]]; then
  COMPOSE_EXTRA+=(-f "${PROJECT_ROOT}/docker/neo4j/test-compose.dev.yml")
fi

DC=(docker compose -f "${COMPOSE_FILE}" "${COMPOSE_EXTRA[@]}" --env-file "${ENV_FILE}")

log() { echo "[$(date +'%H:%M:%S')] $*"; }

compose_has_service() {
  "${DC[@]}" config --services 2>/dev/null | awk -v s="$1" '$0==s{found=1} END{exit found?0:1}'
}

compose_up_if_exists() {
  local svc="$1"
  if compose_has_service "${svc}"; then
    "${DC[@]}" up -d "${svc}"
  else
    log "WARN: service '${svc}' not found in compose file, skipping"
  fi
}

log "Loaded env file: ${ENV_FILE}"
log "Using compose file: ${COMPOSE_FILE}"
log "3-node services: primary=${PRIMARY_SERVICE}, standby1=${STANDBY1_SERVICE}, standby2=${STANDBY2_SERVICE}"
log "Effective Redis config for init: host=${REDIS_HOST}, port=${REDIS_PORT}, password_set=$([[ -n "${REDIS_PASSWORD}" ]] && echo yes || echo no)"
if (( ${#COMPOSE_EXTRA[@]} > 0 )); then
  log "HA Agent build overlay enabled: docker/neo4j/test-compose.dev.yml (set USE_HA_AGENT_BUILD_OVERLAY=0 for registry-only image)"
fi

# ── 1. Stop all containers ───────────────────────────────────────────────────
log "Stopping all containers..."
"${DC[@]}" down --remove-orphans 2>/dev/null || true

# ── 2. Clean Neo4j data (remove stale cluster metadata) ─────────────────────
log "Cleaning Neo4j data & logs..."
rm -rf "${NODE1_DIR}/data/"* "${NODE2_DIR}/data/"* "${NODE3_DIR}/data/"*
rm -rf "${NODE1_DIR}/logs/"* "${NODE2_DIR}/logs/"* "${NODE3_DIR}/logs/"*

# ── 3. Clean Redis HA state ──────────────────────────────────────────────────
# This script wipes Neo4j data (step 2) — matching Redis state must also be wiped so
# the cluster truly starts from zero. Leftover state to clean:
#   neo4j:ha:node-registry             — role / health / serviceState per node
#   neo4j:ha:fencing-token             — monotonically increasing epoch counter
#   neo4j:ha:cdc-checkpoint:<nodeId>   — CDC poll cursor (lastTs, lastElementId, ...)
#   neo4j:ha:sync-checkpoint:<nodeId>  — Stream consumer cursor per standby
#   neo4j:cdc:neo4j:changes            — incremental change Stream
#   neo4j:cdc:neo4j:fullsync           — full-sync Stream
#   neo4j:cdc:neo4j:control            — control event Stream (if used)
#
# Not wiping all of these causes:
#   - CDC resuming from a future-dated cursor → no events captured until a newer
#     _updated_at is written, making the cluster look "frozen"
#   - SyncApplier consuming stale Stream events and logging "stale fencing token" warnings
#   - Service-state evaluator trusting an outdated checkpoint's updatedAt (BUG-033 window)
log "Cleaning Redis HA keys..."
redis_cmd=(redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}")
if [[ -n "${REDIS_PASSWORD}" ]]; then
  redis_cmd+=(-a "${REDIS_PASSWORD}")
fi

redis_run() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "${REDIS_CLI_TIMEOUT_SEC}s" "${redis_cmd[@]}" "$@" 2>/dev/null
  else
    "${redis_cmd[@]}" "$@" 2>/dev/null
  fi
}

redis_del_key() {
  local key="$1"
  redis_run DEL "${key}" >/dev/null \
    || log "WARN: redis DEL ${key} failed/timeout at ${REDIS_HOST}:${REDIS_PORT}"
}

redis_del_pattern() {
  # Delete every key matching a glob pattern using SCAN (safer than KEYS on big DBs).
  local pattern="$1"
  local cursor="0"
  local deleted=0
  while :; do
    local out
    out="$(redis_run SCAN "${cursor}" MATCH "${pattern}" COUNT 100 || true)"
    [[ -z "${out}" ]] && break
    cursor="$(echo "${out}" | head -n 1)"
    local keys
    keys="$(echo "${out}" | tail -n +2)"
    if [[ -n "${keys}" ]]; then
      while IFS= read -r k; do
        [[ -z "${k}" ]] && continue
        redis_run DEL "${k}" >/dev/null || true
        deleted=$(( deleted + 1 ))
      done <<<"${keys}"
    fi
    [[ "${cursor}" == "0" ]] && break
  done
  if (( deleted > 0 )); then
    log "  deleted ${deleted} key(s) matching ${pattern}"
  fi
}

# Single-key deletes (fixed names)
redis_del_key "neo4j:ha:node-registry"
redis_del_key "neo4j:ha:fencing-token"
redis_del_key "neo4j:ha:leader-lock"

# Pattern-based deletes (per-node checkpoints + CDC streams)
redis_del_pattern "neo4j:ha:cdc-checkpoint:*"
redis_del_pattern "neo4j:ha:sync-checkpoint:*"
redis_del_pattern "neo4j:ha:backup-checkpoint"
# BUG-080: pending-reconcile intents from a prior chaos run would be consumed
# by OldPrimaryRecovery Step 4.5 against a cold cluster and waste budget. The
# cold-cursor guard inside the reconciler short-circuits anyway, but wiping
# the keys keeps init state truly empty and avoids confusing debug output.
redis_del_pattern "neo4j:ha:pending-reconcile:*"
redis_del_pattern "neo4j:cdc:*"

# ── 4. Ensure bind-mount directories exist ───────────────────────────────────
log "Ensuring bind-mount directories..."
mkdir -p \
  "${NODE1_DIR}/"{data,logs,import,plugins} \
  "${NODE2_DIR}/"{data,logs,import,plugins} \
  "${NODE3_DIR}/"{data,logs,import,plugins} \
  /opt/haproxy-1/haproxy-1-socket \
  /opt/haproxy-2/haproxy-2-socket \
  /opt/ha-agent/buffer

# ── 5. Start Neo4j 1 primary + 2 standbys ───────────────────────────────────
log "Starting Neo4j primary and 2 standbys..."
"${DC[@]}" up -d "${PRIMARY_SERVICE}" "${STANDBY1_SERVICE}" "${STANDBY2_SERVICE}"

# ── 6. Wait for Neo4j healthy (polling, max 180s) ───────────────────────────
log "Waiting for all Neo4j nodes healthy..."
MAX_WAIT=180
ELAPSED=0
INTERVAL=5
while (( ELAPSED < MAX_WAIT )); do
  primary_health="$(docker inspect "${PRIMARY_CONTAINER}" --format '{{.State.Health.Status}}' 2>/dev/null || echo "missing")"
  standby1_health="$(docker inspect "${STANDBY1_CONTAINER}" --format '{{.State.Health.Status}}' 2>/dev/null || echo "missing")"
  standby2_health="$(docker inspect "${STANDBY2_CONTAINER}" --format '{{.State.Health.Status}}' 2>/dev/null || echo "missing")"

  if [[ "${primary_health}" == "healthy" && "${standby1_health}" == "healthy" && "${standby2_health}" == "healthy" ]]; then
    log "Neo4j healthy after ${ELAPSED}s (primary=${primary_health}, standby1=${standby1_health}, standby2=${standby2_health})"
    break
  fi

  sleep "${INTERVAL}"
  ELAPSED=$(( ELAPSED + INTERVAL ))
  log "  waiting... primary=${primary_health}, standby1=${standby1_health}, standby2=${standby2_health} (${ELAPSED}s/${MAX_WAIT}s)"
done

if (( ELAPSED >= MAX_WAIT )); then
  log "ERROR: Neo4j 3-node cluster did not become healthy within ${MAX_WAIT}s"
  "${DC[@]}" ps
  exit 1
fi

# ── 7. Start HAProxy instances ───────────────────────────────────────────────
log "Starting HAProxy services..."
for svc in ${HAPROXY_SERVICES}; do
  compose_up_if_exists "${svc}"
done

# ── 8. Start HA Agent ────────────────────────────────────────────────────────
log "Starting HA Agent..."
compose_up_if_exists "${AGENT_SERVICE}"

# ── 9. Show final status ─────────────────────────────────────────────────────
sleep 3
log "All services started. Current status:"
"${DC[@]}" ps
log "View HA Agent logs: docker compose -f ${COMPOSE_FILE} --env-file ${ENV_FILE} logs -f ${AGENT_SERVICE}"
