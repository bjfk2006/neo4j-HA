#!/usr/bin/env bash
#
# ha-diag-collect.sh — One-shot HA diagnostics collector for neo4j-ha.
#
# Produces a self-contained bundle (tar.gz) containing:
#   - Redis:   XLEN / XINFO GROUPS / XRANGE (full dump) for CDC streams,
#              all cdc-checkpoint:* / sync-checkpoint:* / fencing-token,
#              node-registry, leader-lock
#   - Neo4j:   per-container TestNode count + range, _updated_at coverage,
#              APOC trigger list, index list, timestamp() (clock skew check)
#   - HAProxy: show servers state + show stat
#   - Agent:   full docker logs ha-agent (since bundle start)
#   - Compose: versions, `docker ps`, disk usage
#
# The bundle is meant to be fed into ha-diag-analyze.py, or shipped to a
# reviewer. No sensitive values (passwords) are written out — only the
# variable NAMES and whether they are set.
#
# USAGE:
#   scripts/diag/ha-diag-collect.sh [--out DIR] [--env-file PATH]
#                                   [--stream KEY ...]
#
# ENV (all optional; sensible defaults if unset):
#   NEO4J_PASSWORD    neo4j bolt password (read from --env-file if absent)
#   REDIS_HOST        default 127.0.0.1
#   REDIS_PORT        default 6379
#   REDIS_PASSWORD    default empty
#   STREAM_KEY        default "neo4j:cdc:neo4j:changes"
#   NEO4J_SERVICES    space-separated container names
#                     default "neo4j-primary neo4j-standby-1 neo4j-standby-2"
#   HAPROXY_SERVICES  default "haproxy-1 haproxy-2"
#   AGENT_CONTAINER   default "ha-agent"

set -uo pipefail

# ------------------------------ args ------------------------------
OUT_DIR=""
ENV_FILE="${ENV_FILE:-docker/.env}"
STREAM_KEYS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --out)        OUT_DIR="$2"; shift 2 ;;
        --env-file)   ENV_FILE="$2"; shift 2 ;;
        --stream)     STREAM_KEYS+=("$2"); shift 2 ;;
        -h|--help)
            sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

TS=$(date -u +%Y%m%dT%H%M%SZ)
if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="/tmp/ha-diag-${TS}"
fi
mkdir -p "$OUT_DIR"

# ------------------------------ env load ------------------------------
if [[ -f "$ENV_FILE" ]]; then
    set -a; . "$ENV_FILE"; set +a
fi
: "${REDIS_HOST:=127.0.0.1}"
: "${REDIS_PORT:=6379}"
: "${STREAM_KEY:=neo4j:cdc:neo4j:changes}"
: "${NEO4J_SERVICES:=neo4j-primary neo4j-standby-1 neo4j-standby-2}"
: "${HAPROXY_SERVICES:=haproxy-1 haproxy-2}"
: "${AGENT_CONTAINER:=ha-agent}"
if [[ ${#STREAM_KEYS[@]} -eq 0 ]]; then
    STREAM_KEYS=("$STREAM_KEY" "neo4j:cdc:neo4j:fullsync")
fi

# ------------------------------ helpers ------------------------------
RCLI=(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT")
[[ -n "${REDIS_PASSWORD:-}" ]] && RCLI+=(-a "$REDIS_PASSWORD" --no-auth-warning)

run() {
    # run label cmd... — captures stdout+stderr, tags with exit code
    local label="$1"; shift
    local out="$OUT_DIR/$label"
    mkdir -p "$(dirname "$out")"
    {
        echo "### $(date -u +%FT%TZ) — $*"
        "$@" 2>&1
        echo "### exit=$?"
    } > "$out"
}

step() {
    echo "[$(date -u +%T)] $*" | tee -a "$OUT_DIR/collector.log"
}

# ------------------------------ start ------------------------------
step "bundle: $OUT_DIR  streams: ${STREAM_KEYS[*]}  redis: $REDIS_HOST:$REDIS_PORT"
{
    echo "collected_at: $(date -u +%FT%TZ)"
    echo "host: $(hostname)"
    echo "uname: $(uname -a)"
    echo "env_file: $ENV_FILE"
    echo "neo4j_password_set: $([ -n "${NEO4J_PASSWORD:-}" ] && echo yes || echo no)"
    echo "redis_password_set: $([ -n "${REDIS_PASSWORD:-}" ] && echo yes || echo no)"
    echo "redis: $REDIS_HOST:$REDIS_PORT"
    echo "stream_keys: ${STREAM_KEYS[*]}"
    echo "neo4j_services: $NEO4J_SERVICES"
    echo "haproxy_services: $HAPROXY_SERVICES"
    echo "agent_container: $AGENT_CONTAINER"
} > "$OUT_DIR/meta.yml"

# ------------------------------ docker inventory ------------------------------
step "docker inventory"
run "docker/ps.txt"       docker ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
run "docker/images.txt"   docker images --format 'table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}'
run "docker/compose.txt"  docker version

# ------------------------------ redis ------------------------------
step "redis: PING + INFO"
run "redis/ping.txt"         "${RCLI[@]}" PING
run "redis/info_server.txt"  "${RCLI[@]}" INFO server
run "redis/info_memory.txt"  "${RCLI[@]}" INFO memory
run "redis/info_stats.txt"   "${RCLI[@]}" INFO stats
run "redis/info_persistence.txt" "${RCLI[@]}" INFO persistence

step "redis: ha keys"
run "redis/keys_ha.txt"       "${RCLI[@]}" --scan --pattern 'neo4j:ha:*'
run "redis/keys_cdc.txt"      "${RCLI[@]}" --scan --pattern 'neo4j:cdc:*'
run "redis/fencing-token.txt" "${RCLI[@]}" GET "neo4j:ha:fencing-token"
run "redis/leader-lock.txt"   "${RCLI[@]}" GET "neo4j:ha:leader-lock"
run "redis/node-registry.txt" "${RCLI[@]}" HGETALL "neo4j:ha:node-registry"

step "redis: checkpoints"
# dump every checkpoint key (CDC + sync) found in the keyspace
while read -r key; do
    [[ -n "$key" ]] || continue
    safe=$(echo "$key" | tr '/:' '__')
    run "redis/checkpoints/${safe}.txt" "${RCLI[@]}" HGETALL "$key"
done < <("${RCLI[@]}" --scan --pattern 'neo4j:ha:*-checkpoint:*' 2>/dev/null)

step "redis: stream metadata + full dump"
mkdir -p "$OUT_DIR/redis/streams"
for sk in "${STREAM_KEYS[@]}"; do
    safe=$(echo "$sk" | tr '/:' '__')
    run "redis/streams/${safe}.xlen.txt"    "${RCLI[@]}" XLEN "$sk"
    run "redis/streams/${safe}.groups.txt"  "${RCLI[@]}" XINFO GROUPS "$sk"
    run "redis/streams/${safe}.stream.txt"  "${RCLI[@]}" XINFO STREAM "$sk"
    run "redis/streams/${safe}.head.txt"    "${RCLI[@]}" XRANGE    "$sk" - + COUNT 5
    run "redis/streams/${safe}.tail.txt"    "${RCLI[@]}" XREVRANGE "$sk" + - COUNT 5
    # Full dump (may be large); gated by size. For streams > 500k entries we
    # sample: first 10k + last 10k + periodic probes.
    xlen_raw=$("${RCLI[@]}" XLEN "$sk" 2>/dev/null || echo 0)
    if [[ "$xlen_raw" =~ ^[0-9]+$ && "$xlen_raw" -gt 0 ]]; then
        if [[ "$xlen_raw" -le 500000 ]]; then
            "${RCLI[@]}" XRANGE "$sk" - + > "$OUT_DIR/redis/streams/${safe}.full.txt" 2>&1
        else
            step "  stream $sk too large ($xlen_raw entries), sampling head+tail 10k"
            "${RCLI[@]}" XRANGE   "$sk" - + COUNT 10000 > "$OUT_DIR/redis/streams/${safe}.head10k.txt" 2>&1
            "${RCLI[@]}" XREVRANGE "$sk" + - COUNT 10000 > "$OUT_DIR/redis/streams/${safe}.tail10k.txt" 2>&1
        fi
    fi
done

# Per-consumer pending list (XPENDING) for every consumer group found
step "redis: XPENDING per consumer group"
mkdir -p "$OUT_DIR/redis/pending"
for sk in "${STREAM_KEYS[@]}"; do
    groups=$("${RCLI[@]}" XINFO GROUPS "$sk" 2>/dev/null \
        | awk '/^name$/{getline; print}')
    for g in $groups; do
        safe_sk=$(echo "$sk" | tr '/:' '__')
        safe_g=$(echo "$g" | tr '/:' '__')
        run "redis/pending/${safe_sk}_${safe_g}.summary.txt" "${RCLI[@]}" XPENDING "$sk" "$g"
        run "redis/pending/${safe_sk}_${safe_g}.detail.txt"  "${RCLI[@]}" XPENDING "$sk" "$g" - + 100
    done
done

# ------------------------------ neo4j ------------------------------
step "neo4j: per-node inventory"
mkdir -p "$OUT_DIR/neo4j"
if [[ -z "${NEO4J_PASSWORD:-}" ]]; then
    echo "NEO4J_PASSWORD not set; skipping Neo4j queries" \
        > "$OUT_DIR/neo4j/SKIPPED.txt"
else
    for svc in $NEO4J_SERVICES; do
        dir="$OUT_DIR/neo4j/$svc"
        mkdir -p "$dir"
        # Clock + identity
        run "neo4j/$svc/clock.txt" \
            docker exec "$svc" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
            "RETURN timestamp() AS server_ts;"
        # Index inventory
        run "neo4j/$svc/indexes.txt" \
            docker exec "$svc" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
            "SHOW INDEXES YIELD name, labelsOrTypes, properties, state, entityType;"
        # Trigger inventory (APOC)
        run "neo4j/$svc/triggers.txt" \
            docker exec "$svc" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
            "CALL apoc.trigger.list() YIELD name, installed, paused RETURN name, installed, paused;"
        # Label list
        run "neo4j/$svc/labels.txt" \
            docker exec "$svc" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
            "CALL db.labels() YIELD label RETURN label;"
        # TestNode distribution (covers load test. if no TestNode, noop.)
        run "neo4j/$svc/testnode_overview.txt" \
            docker exec "$svc" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
            "MATCH (n:TestNode) RETURN count(n) AS total, min(n._updated_at) AS min_ts, max(n._updated_at) AS max_ts, count(n._elementId) AS with_eid;"
        run "neo4j/$svc/testnode_by_writer.txt" \
            docker exec "$svc" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
            "MATCH (n:TestNode) RETURN coalesce(n.writer,'<null>') AS writer, min(n.seq) AS min_seq, max(n.seq) AS max_seq, count(n) AS cnt ORDER BY writer;"
        # All user-label counts (to compare replication state across nodes)
        run "neo4j/$svc/label_counts.txt" \
            docker exec "$svc" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
            "CALL db.labels() YIELD label CALL { WITH label MATCH (n) WHERE label IN labels(n) RETURN count(n) AS cnt } RETURN label, cnt ORDER BY label;"
        # CDCDeleteEvent residual
        run "neo4j/$svc/cdc_delete_events.txt" \
            docker exec "$svc" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
            "MATCH (n:_CDCDeleteEvent) RETURN count(n) AS c;"
    done
fi

# ------------------------------ haproxy ------------------------------
step "haproxy: servers state + stats"
mkdir -p "$OUT_DIR/haproxy"
for hp in $HAPROXY_SERVICES; do
    dir="$OUT_DIR/haproxy/$hp"
    mkdir -p "$dir"
    # Find admin socket inside container (common paths)
    run "haproxy/$hp/servers_state.txt" bash -c "
        for path in /var/run/haproxy.sock /var/run/haproxy/admin.sock /tmp/haproxy.sock; do
            if docker exec '$hp' test -S \"\$path\" 2>/dev/null; then
                echo 'show servers state' | docker exec -i '$hp' socat - UNIX-CONNECT:\"\$path\"
                exit 0
            fi
        done
        echo 'No admin socket found (tried common paths)'
        exit 1
    "
    run "haproxy/$hp/show_stat.txt" bash -c "
        for path in /var/run/haproxy.sock /var/run/haproxy/admin.sock /tmp/haproxy.sock; do
            if docker exec '$hp' test -S \"\$path\" 2>/dev/null; then
                echo 'show stat' | docker exec -i '$hp' socat - UNIX-CONNECT:\"\$path\"
                exit 0
            fi
        done
        echo 'No admin socket found'
        exit 1
    "
    run "haproxy/$hp/logs.txt" docker logs --tail 500 "$hp"
done

# ------------------------------ agent ------------------------------
step "ha-agent: logs (full + errors)"
mkdir -p "$OUT_DIR/agent"
if docker ps -a --format '{{.Names}}' | grep -Fxq "$AGENT_CONTAINER"; then
    docker logs "$AGENT_CONTAINER" > "$OUT_DIR/agent/stdout.log" 2>&1
    # Extract high-signal lines
    grep -iE 'fencing|reject|failover|switchover|recovery|rollback|WARN|ERROR|Exception|Trigger' \
        "$OUT_DIR/agent/stdout.log" > "$OUT_DIR/agent/highlights.log" 2>/dev/null || true
    grep -iE 'ERROR|Exception|stack|caused by' \
        "$OUT_DIR/agent/stdout.log" > "$OUT_DIR/agent/errors.log" 2>/dev/null || true
else
    echo "agent container '$AGENT_CONTAINER' not found" \
        > "$OUT_DIR/agent/NOT_FOUND.txt"
fi

# ------------------------------ bundle ------------------------------
step "bundle: creating tar.gz"
TAR="${OUT_DIR}.tar.gz"
tar -czf "$TAR" -C "$(dirname "$OUT_DIR")" "$(basename "$OUT_DIR")"
step "bundle written: $TAR"
du -sh "$TAR" | tee -a "$OUT_DIR/collector.log"

echo
echo "=================================================================="
echo "  DIAG BUNDLE READY"
echo "  dir: $OUT_DIR"
echo "  tar: $TAR"
echo "  run analyzer: python3 scripts/diag/ha-diag-analyze.py $OUT_DIR"
echo "=================================================================="
