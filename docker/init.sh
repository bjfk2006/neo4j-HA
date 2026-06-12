#!/usr/bin/env bash
# Allow invocation via `sh docker/init.sh` by re-execing with bash.
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${PROJECT_ROOT}/docker/neo4j/test-compose.yml}"
ENV_FILE="${PROJECT_ROOT}/docker/.env"
USE_HA_AGENT_BUILD_OVERLAY="${USE_HA_AGENT_BUILD_OVERLAY:-1}"
COMPOSE_EXTRA=()
if [[ "${USE_HA_AGENT_BUILD_OVERLAY}" == "1" && -f "${PROJECT_ROOT}/docker/neo4j/test-compose.dev.yml" ]]; then
  COMPOSE_EXTRA+=(-f "${PROJECT_ROOT}/docker/neo4j/test-compose.dev.yml")
fi

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

dcompose() { docker compose -f "${COMPOSE_FILE}" "${COMPOSE_EXTRA[@]}" --env-file "${ENV_FILE}" "$@"; }

log() { echo "[$(date +'%H:%M:%S')] $*"; }

log "Loaded env file: ${ENV_FILE}"
log "Effective Redis config for init: host=${REDIS_HOST}, port=${REDIS_PORT}, password_set=$([[ -n "${REDIS_PASSWORD}" ]] && echo yes || echo no)"
if (( ${#COMPOSE_EXTRA[@]} > 0 )); then
  log "HA Agent build overlay enabled: docker/neo4j/test-compose.dev.yml (set USE_HA_AGENT_BUILD_OVERLAY=0 for registry-only image)"
fi

# ── 1. Stop all containers ───────────────────────────────────────────────────
log "Stopping all containers..."
dcompose down --remove-orphans 2>/dev/null || true

# ── 2. Clean Neo4j data (remove stale cluster metadata) ─────────────────────
log "Cleaning Neo4j data & logs..."
rm -rf /opt/neo4j-node1/data/* /opt/neo4j-node2/data/*
rm -rf /opt/neo4j-node1/logs/* /opt/neo4j-node2/logs/*

# ── 3. Clean Redis HA state ──────────────────────────────────────────────────
# Wipe the full set of HA + CDC keys so the cluster truly starts from zero. Leaving
# stale checkpoints or Stream entries behind causes CDC to resume from a future-dated
# cursor (frozen cluster) or SyncApplier to consume stale-fencing-token events.
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

redis_del_key "neo4j:ha:node-registry"
redis_del_key "neo4j:ha:fencing-token"
redis_del_key "neo4j:ha:leader-lock"
redis_del_pattern "neo4j:ha:cdc-checkpoint:*"
redis_del_pattern "neo4j:ha:sync-checkpoint:*"
redis_del_pattern "neo4j:ha:backup-checkpoint"
redis_del_pattern "neo4j:cdc:*"

# ── 4. Ensure bind-mount directories exist ───────────────────────────────────
log "Ensuring bind-mount directories..."
mkdir -p \
  /opt/neo4j-node1/{data,logs,import,plugins} \
  /opt/neo4j-node2/{data,logs,import,plugins} \
  /opt/haproxy-1/haproxy-1-socket \
  /opt/haproxy-2/haproxy-2-socket \
  /opt/ha-agent/buffer

# ── 5. Start Neo4j ───────────────────────────────────────────────────────────
log "Starting Neo4j primary & standby..."
dcompose up -d neo4j-primary neo4j-standby

# ── 6. Wait for Neo4j healthy (polling, max 120s) ───────────────────────────
log "Waiting for Neo4j healthy..."
MAX_WAIT=120
ELAPSED=0
INTERVAL=5
while (( ELAPSED < MAX_WAIT )); do
  primary_health="$(docker inspect neo4j-primary --format '{{.State.Health.Status}}' 2>/dev/null || echo "missing")"
  standby_health="$(docker inspect neo4j-standby --format '{{.State.Health.Status}}' 2>/dev/null || echo "missing")"
  if [[ "${primary_health}" == "healthy" && "${standby_health}" == "healthy" ]]; then
    log "Neo4j healthy (primary=${primary_health}, standby=${standby_health}) after ${ELAPSED}s"
    break
  fi
  sleep ${INTERVAL}
  ELAPSED=$(( ELAPSED + INTERVAL ))
  log "  waiting... primary=${primary_health}, standby=${standby_health} (${ELAPSED}s/${MAX_WAIT}s)"
done

if (( ELAPSED >= MAX_WAIT )); then
  log "ERROR: Neo4j did not become healthy within ${MAX_WAIT}s"
  dcompose ps
  exit 1
fi

# ── 7. Start HAProxy ────────────────────────────────────────────────────────
log "Starting HAProxy..."
dcompose up -d haproxy-1 haproxy-2

# ── 8. Start HA Agent ────────────────────────────────────────────────────────
log "Starting HA Agent..."
dcompose up -d ha-agent

# ── 9. Show final status ────────────────────────────────────────────────────
sleep 3
log "All services started. Current status:"
dcompose ps
log "View HA Agent logs: docker compose -f ${COMPOSE_FILE} ${COMPOSE_EXTRA[*]:+${COMPOSE_EXTRA[*]} }--env-file ${ENV_FILE} logs -f ha-agent"
