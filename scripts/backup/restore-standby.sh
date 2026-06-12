#!/usr/bin/env bash
set -euo pipefail

# Standby restore via `neo4j-admin database restore`.
#
# Usage:
#   ADMIN_TOKEN=xxx [env...] restore-standby.sh <backup-archive.tar.gz>
#
# Flow:
#   1) Verify HA Agent healthy + target node role == STANDBY
#   2) Verify archive (optional sha256) and extract to host temp dir
#   3) (Optional) Drain target from HAProxy read backend (state=maint)
#   4) Stop the standby container
#   5) Run `neo4j-admin database restore` via an ephemeral container that
#      mounts the same data volume + the extracted backup directory
#   6) Start the standby container and wait for healthy
#   7) POST /cluster/fullsync?nodeId=... to force consistent catch-up
#   8) Wait for syncLag to recover, then (optional) restore HAProxy read backend
#
# Signal/ERR safe: failure/interrupt triggers best-effort start-container +
# restore of read backend so the cluster won't be left in a dangerous state.

HA_AGENT_URL="${HA_AGENT_URL:-http://localhost:8080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

STANDBY_NODE_ID="${STANDBY_NODE_ID:-node-02}"                   # HA Agent cluster node id
STANDBY_CONTAINER="${STANDBY_CONTAINER:-neo4j-standby-1}"        # docker container name
STANDBY_DATA_HOST_DIR="${STANDBY_DATA_HOST_DIR:-/opt/neo4j-node2/data}"
NEO4J_IMAGE="${NEO4J_IMAGE:-neo4j:2026.02.3}"
DATABASE_NAME="${DATABASE_NAME:-neo4j}"

OVERWRITE_DESTINATION="${OVERWRITE_DESTINATION:-true}"
HEALTH_WAIT_SECONDS="${HEALTH_WAIT_SECONDS:-120}"
SYNC_LAG_THRESHOLD_MS="${SYNC_LAG_THRESHOLD_MS:-5000}"
SYNC_WAIT_SECONDS="${SYNC_WAIT_SECONDS:-600}"

DRAIN_READ_BACKEND="${DRAIN_READ_BACKEND:-1}"
HAPROXY_SOCKETS_DEFAULT="/opt/haproxy-1/haproxy-1-socket/admin.sock /opt/haproxy-2/haproxy-2-socket/admin.sock"
HAPROXY_SOCKETS="${HAPROXY_SOCKETS:-$HAPROXY_SOCKETS_DEFAULT}"
HAPROXY_READ_BACKEND="${HAPROXY_READ_BACKEND:-neo4j_all}"
HAPROXY_SERVER_NAME="${HAPROXY_SERVER_NAME:-$STANDBY_CONTAINER}"

log() { echo "[$(date +'%H:%M:%S')] $*"; }
die() { echo "[ERROR] $*" >&2; exit 1; }

ha_agent_get()  { curl -fsS "${HA_AGENT_URL}$1"; }
ha_agent_post() { curl -fsS -X POST "${HA_AGENT_URL}$1" -H "Authorization: Bearer ${ADMIN_TOKEN}"; }

haproxy_cmd() {
  local sock="$1" cmd="$2"
  python3 - "$sock" "$cmd" <<'PY'
import socket, sys
s=socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sys.argv[1])
s.sendall((sys.argv[2]+"\n").encode())
buf=b""
while True:
    c=s.recv(4096)
    if not c: break
    buf+=c
s.close()
out=buf.decode("utf-8", errors="ignore").strip()
if out: print(out)
PY
}

set_read_state() {
  local state="$1" sock
  for sock in ${HAPROXY_SOCKETS}; do
    if [[ -S "${sock}" ]]; then
      haproxy_cmd "${sock}" "set server ${HAPROXY_READ_BACKEND}/${HAPROXY_SERVER_NAME} state ${state}" || true
    else
      log "HAProxy socket not found, skip: ${sock}"
    fi
  done
}

check_role_standby() {
  local status role
  status="$(ha_agent_get "/cluster/status")"
  role="$(python3 -c 'import json,sys
d=json.loads(sys.stdin.read() or "{}")
t=sys.argv[1]
for n in d.get("nodes",[]):
    if n.get("id")==t:
        print(n.get("role",""))
        break' "${STANDBY_NODE_ID}" <<<"${status}")"
  [[ "${role}" == "STANDBY" ]] || die "Node ${STANDBY_NODE_ID} role is '${role:-UNKNOWN}', expected STANDBY"
}

wait_container_healthy() {
  local max="${1:-120}" i=0 status
  while (( i < max )); do
    status="$(docker inspect -f '{{.State.Health.Status}}' "${STANDBY_CONTAINER}" 2>/dev/null || echo "unknown")"
    if [[ "${status}" == "healthy" ]]; then return 0; fi
    sleep 2; i=$((i+2))
  done
  return 1
}

wait_sync_caught_up() {
  local max="${1:-600}" i=0 lag
  while (( i < max )); do
    lag="$(ha_agent_get "/cluster/status" | python3 -c 'import json,sys
d=json.loads(sys.stdin.read() or "{}")
t=sys.argv[1]
for n in d.get("nodes",[]):
    if n.get("id")==t:
        print(n.get("syncLagMs","99999999"))
        break' "${STANDBY_NODE_ID}")"
    if [[ -n "${lag}" ]] && (( lag < SYNC_LAG_THRESHOLD_MS )); then
      return 0
    fi
    log "Waiting for ${STANDBY_NODE_ID} sync catch-up (lag=${lag}ms)"
    sleep 5; i=$((i+5))
  done
  return 1
}

# Auto-load docker/.env for ADMIN_TOKEN when missing
if [[ -z "${ADMIN_TOKEN}" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
  ENV_FILE="${PROJECT_ROOT}/docker/.env"
  if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
    ADMIN_TOKEN="${ADMIN_TOKEN:-}"
  fi
fi

BACKUP_ARCHIVE="${1:-}"
[[ -n "${ADMIN_TOKEN}" ]]       || die "ADMIN_TOKEN is required"
[[ -n "${BACKUP_ARCHIVE}" ]]    || die "Usage: $0 <backup-archive.tar.gz>"
[[ -f "${BACKUP_ARCHIVE}" ]]    || die "Backup archive not found: ${BACKUP_ARCHIVE}"
[[ -d "${STANDBY_DATA_HOST_DIR}" ]] || die "Standby data dir not found: ${STANDBY_DATA_HOST_DIR}"

command -v docker    >/dev/null || die "docker not found"
command -v curl      >/dev/null || die "curl not found"
command -v python3   >/dev/null || die "python3 not found"
command -v tar       >/dev/null || die "tar not found"
command -v sha256sum >/dev/null || die "sha256sum not found"

drained="false"
container_stopped="false"
TMP_RESTORE_DIR="$(mktemp -d -t neo4j-restore.XXXXXX)"

cleanup() {
  local rc=$?
  if [[ "${container_stopped}" == "true" ]]; then
    log "Starting standby container (best-effort)"
    docker start "${STANDBY_CONTAINER}" >/dev/null 2>&1 || true
    container_stopped="false"
  fi
  if [[ "${drained}" == "true" ]]; then
    log "Restoring HAProxy read state to ready (best-effort)"
    set_read_state "ready" || true
    drained="false"
  fi
  rm -rf "${TMP_RESTORE_DIR}" 2>/dev/null || true
  exit "${rc}"
}
trap cleanup EXIT INT TERM

log "HA Agent health check"
ha_agent_get "/health" | grep -q UP || die "HA Agent /health is not UP"

log "Verify target node is STANDBY: ${STANDBY_NODE_ID}"
check_role_standby

if [[ -f "${BACKUP_ARCHIVE}.sha256" ]]; then
  log "Verify archive checksum"
  ( cd "$(dirname "${BACKUP_ARCHIVE}")" \
      && sha256sum -c "$(basename "${BACKUP_ARCHIVE}").sha256" )
else
  log "No .sha256 sidecar found, skipping checksum verification"
fi

log "Extract archive to: ${TMP_RESTORE_DIR}"
tar -xzf "${BACKUP_ARCHIVE}" -C "${TMP_RESTORE_DIR}"
RESTORE_SRC_DIR="$(find "${TMP_RESTORE_DIR}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
[[ -d "${RESTORE_SRC_DIR}" ]] || die "Extracted backup contents not found under ${TMP_RESTORE_DIR}"

if [[ "${DRAIN_READ_BACKEND}" == "1" ]]; then
  log "Drain ${HAPROXY_SERVER_NAME} from HAProxy read backend"
  set_read_state "maint"
  drained="true"
fi

log "Stop standby container: ${STANDBY_CONTAINER}"
docker stop "${STANDBY_CONTAINER}" >/dev/null
container_stopped="true"

log "Run neo4j-admin database restore via ephemeral container"
docker run --rm \
  -v "${STANDBY_DATA_HOST_DIR}:/data" \
  -v "${RESTORE_SRC_DIR}:/restore:ro" \
  --entrypoint neo4j-admin \
  "${NEO4J_IMAGE}" \
  database restore "${DATABASE_NAME}" \
    --from-path=/restore \
    --overwrite-destination="${OVERWRITE_DESTINATION}"

log "Start standby container"
docker start "${STANDBY_CONTAINER}" >/dev/null
container_stopped="false"

log "Wait for ${STANDBY_CONTAINER} to become healthy (max ${HEALTH_WAIT_SECONDS}s)"
wait_container_healthy "${HEALTH_WAIT_SECONDS}" \
  || die "Standby container did not become healthy within ${HEALTH_WAIT_SECONDS}s"

log "Trigger full sync for ${STANDBY_NODE_ID} to guarantee consistency"
ha_agent_post "/cluster/fullsync?nodeId=${STANDBY_NODE_ID}" >/dev/null

log "Wait for ${STANDBY_NODE_ID} sync lag < ${SYNC_LAG_THRESHOLD_MS}ms (max ${SYNC_WAIT_SECONDS}s)"
if ! wait_sync_caught_up "${SYNC_WAIT_SECONDS}"; then
  log "[WARN] Sync did not catch up within ${SYNC_WAIT_SECONDS}s; continue monitoring with /cluster/status"
fi

if [[ "${DRAIN_READ_BACKEND}" == "1" ]]; then
  log "Restore ${HAPROXY_SERVER_NAME} to HAProxy read backend"
  set_read_state "ready"
  drained="false"
fi

log "[OK] Restore finished for ${STANDBY_NODE_ID} from ${BACKUP_ARCHIVE}"
