#!/usr/bin/env bash
set -euo pipefail

# Online standby backup for Neo4j HA via `neo4j-admin database backup`.
#
# Flow:
#   1) Verify HA Agent is healthy and no backup is currently in progress
#   2) Verify target node is a STANDBY (never back up PRIMARY via this flow)
#   3) POST /cluster/backup/prepare   (pause SyncApplier, freeze backup window)
#   4) (Optional) Drain target from HAProxy read backend (state=maint)
#   5) Run `neo4j-admin database backup` inside the standby container
#   6) Copy backup artifacts out of the container, archive and sha256
#   7) (Optional) Restore target to HAProxy read backend (state=ready)
#   8) POST /cluster/backup/complete  (resume SyncApplier)
#   9) Retention cleanup
#
# Signal/ERR safe: any failure triggers best-effort restore of read backend
# and backup/complete so the cluster won't be left in a half-paused state.

HA_AGENT_URL="${HA_AGENT_URL:-http://localhost:8080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"

STANDBY_NODE_ID="${STANDBY_NODE_ID:-node-02}"             # HA Agent cluster node id
STANDBY_CONTAINER="${STANDBY_CONTAINER:-neo4j-standby-1}" # docker container name
DATABASE_NAME="${DATABASE_NAME:-neo4j}"
NEO4J_BACKUP_ADDRESS="${NEO4J_BACKUP_ADDRESS:-localhost:6362}"

BACKUP_ROOT="${BACKUP_ROOT:-/backup/neo4j}"
CONTAINER_BACKUP_DIR="${CONTAINER_BACKUP_DIR:-/tmp/neo4j-backup}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"

DRAIN_READ_BACKEND="${DRAIN_READ_BACKEND:-0}"  # 1 = remove from HAProxy read during backup
HAPROXY_SOCKETS_DEFAULT="/opt/haproxy-1/haproxy-1-socket/admin.sock /opt/haproxy-2/haproxy-2-socket/admin.sock"
HAPROXY_SOCKETS="${HAPROXY_SOCKETS:-$HAPROXY_SOCKETS_DEFAULT}"
HAPROXY_READ_BACKEND="${HAPROXY_READ_BACKEND:-neo4j_all}"
HAPROXY_SERVER_NAME="${HAPROXY_SERVER_NAME:-$STANDBY_CONTAINER}"

log() { echo "[$(date +'%H:%M:%S')] $*"; }
die() { echo "[ERROR] $*" >&2; exit 1; }

# Auto-load docker/.env for ADMIN_TOKEN when missing (same pattern as ha-smoke-test.sh)
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

[[ -n "${ADMIN_TOKEN}" ]] || die "ADMIN_TOKEN is required"
command -v docker  >/dev/null || die "docker not found"
command -v curl    >/dev/null || die "curl not found"
command -v python3 >/dev/null || die "python3 not found"
command -v tar     >/dev/null || die "tar not found"
command -v sha256sum >/dev/null || die "sha256sum not found"

mkdir -p "${BACKUP_ROOT}"
TS="$(date +%Y%m%d-%H%M%S)"
BACKUP_DIR_HOST="${BACKUP_ROOT}/${STANDBY_NODE_ID}-${TS}"
BACKUP_ARCHIVE="${BACKUP_ROOT}/${STANDBY_NODE_ID}-${TS}.tar.gz"

prepared="false"
drained="false"

ha_agent_get()  { curl -fsS "${HA_AGENT_URL}$1"; }
ha_agent_post() { curl -fsS -X POST "${HA_AGENT_URL}$1" -H "Authorization: Bearer ${ADMIN_TOKEN}"; }

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

check_backup_idle() {
  local st state
  st="$(ha_agent_get "/cluster/backup/status")"
  state="$(python3 -c 'import json,sys
print(json.loads(sys.stdin.read() or "{}").get("state",""))' <<<"${st}")"
  [[ "${state}" == "IDLE" ]] || die "Backup not idle, current state=${state:-UNKNOWN}"
}

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
if out:
    print(out)
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

cleanup() {
  local rc=$?
  if [[ "${drained}" == "true" ]]; then
    log "Restoring HAProxy read state to ready (best-effort)"
    set_read_state "ready" || true
    drained="false"
  fi
  if [[ "${prepared}" == "true" ]]; then
    log "Completing backup via HA Agent (best-effort)"
    ha_agent_post "/cluster/backup/complete" >/dev/null || true
    prepared="false"
  fi
  exit "${rc}"
}
trap cleanup EXIT INT TERM

log "HA Agent health check"
ha_agent_get "/health" | grep -q UP || die "HA Agent /health is not UP"

log "Verify target node is STANDBY: ${STANDBY_NODE_ID}"
check_role_standby

log "Verify no backup in progress"
check_backup_idle

log "Prepare backup (pause sync) for ${STANDBY_NODE_ID}"
ha_agent_post "/cluster/backup/prepare?nodeId=${STANDBY_NODE_ID}" >/dev/null
prepared="true"

if [[ "${DRAIN_READ_BACKEND}" == "1" ]]; then
  log "Drain ${HAPROXY_SERVER_NAME} from HAProxy read backend"
  set_read_state "maint"
  drained="true"
fi

log "Prepare container backup dir: ${CONTAINER_BACKUP_DIR}"
docker exec "${STANDBY_CONTAINER}" sh -c "rm -rf '${CONTAINER_BACKUP_DIR}' && mkdir -p '${CONTAINER_BACKUP_DIR}'"

log "Run neo4j-admin database backup inside ${STANDBY_CONTAINER}"
docker exec "${STANDBY_CONTAINER}" \
  neo4j-admin database backup \
  "${DATABASE_NAME}" \
  --from-address="${NEO4J_BACKUP_ADDRESS}" \
  --to-path="${CONTAINER_BACKUP_DIR}"

log "Copy backup out to host: ${BACKUP_DIR_HOST}"
mkdir -p "${BACKUP_DIR_HOST}"
docker cp "${STANDBY_CONTAINER}:${CONTAINER_BACKUP_DIR}/." "${BACKUP_DIR_HOST}/"
docker exec "${STANDBY_CONTAINER}" rm -rf "${CONTAINER_BACKUP_DIR}" || true

log "Archive and checksum"
tar -C "${BACKUP_ROOT}" -czf "${BACKUP_ARCHIVE}" "$(basename "${BACKUP_DIR_HOST}")"
rm -rf "${BACKUP_DIR_HOST}"
( cd "${BACKUP_ROOT}" && sha256sum "$(basename "${BACKUP_ARCHIVE}")" > "$(basename "${BACKUP_ARCHIVE}").sha256" )

if [[ "${DRAIN_READ_BACKEND}" == "1" ]]; then
  log "Restore ${HAPROXY_SERVER_NAME} to HAProxy read backend"
  set_read_state "ready"
  drained="false"
fi

log "Complete backup (resume sync)"
ha_agent_post "/cluster/backup/complete" >/dev/null
prepared="false"

log "Retention cleanup: files older than ${RETENTION_DAYS} days"
find "${BACKUP_ROOT}" -maxdepth 1 -type f \
  \( -name "*.tar.gz" -o -name "*.tar.gz.sha256" \) \
  -mtime +"${RETENTION_DAYS}" -delete || true

log "[OK] Backup finished: ${BACKUP_ARCHIVE}"
log "     Checksum:       ${BACKUP_ARCHIVE}.sha256"
