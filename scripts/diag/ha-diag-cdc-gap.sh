#!/usr/bin/env bash
#
# ha-diag-cdc-gap.sh — Pinpoint a CDC replication gap after a failed
# load-switchover integrity check.
#
# WHEN TO USE THIS
# ================
# Run this right after `ha-load-switchover-test.py` reports symptoms like:
#
#     node-02   FAIL  count=2104 (exp=2174)  miss=70  extra=0 ...
#     node-03   FAIL  count=2104 (exp=2174)  miss=70  extra=0 ...
#
# i.e. one or more standbys are missing a specific batch of TestNodes.
# This script answers three questions in order:
#
#   Q1. WHICH nodes are missing?
#       → set-diff each standby's TestNode set against the primary's,
#         keyed by `id` (writer-id scoped).
#
#   Q2. WHEN were they written?
#       → bucket `_created_at` by second. A tight window (a few seconds)
#         usually aligns with a switchover boundary, pointing at a CDC
#         tail that was never published.
#
#   Q3. WHERE in the pipeline did they drop?
#       → for each missing elementId, scan the Redis XRANGE dump for
#         `NODE_CREATED` payloads. Three outcomes:
#           (a) Missing elementId NOT in stream   → producer side
#               (old primary did not publish it — BUG-061 class:
#                afterAsync tail lost, or new-primary collector
#                baseline skipped it).
#           (b) Missing elementId IS in stream    → consumer side
#               (SyncApplier skipped / failed the batch; check XPENDING
#                and ha-agent applier logs).
#           (c) Mixed                             → report both counts.
#
# USAGE
#   scripts/diag/ha-diag-cdc-gap.sh --writer-id w-245386 \
#       [--out DIR]                 # default /tmp/cdc-gap-<TS>
#       [--env-file PATH]           # default docker/.env
#       [--agent-log-lines N]       # default 2000
#       [--stream KEY]              # default neo4j:cdc:neo4j:changes
#       [--quiesce-seconds N]       # default 0; wait N seconds before sampling
#                                   # (gives sync-applier time to drain the
#                                   # Redis tail after a load test, so we don't
#                                   # flag transient replication lag as a gap)
#
# ENV (optional; loaded from --env-file if present)
#   NEO4J_PASSWORD     required; script aborts without it
#   REDIS_HOST         default 127.0.0.1
#   REDIS_PORT         default 6379
#   REDIS_PASSWORD     default empty
#   NEO4J_SERVICES     default "neo4j-primary neo4j-standby-1 neo4j-standby-2"
#   AGENT_CONTAINER    default "ha-agent"
#
# OUTPUTS (in --out DIR)
#   meta.yml                             run metadata
#   neo4j/<svc>/testnodes.tsv            id<TAB>elementId<TAB>_created_at<TAB>_updated_at<TAB>community
#   neo4j/<svc>/rels.tsv                 elementId<TAB>_created_at<TAB>_updated_at<TAB>type<TAB>from_id<TAB>to_id
#   gaps/nodes_missing_on_<svc>.tsv      rows from primary absent on <svc>
#   gaps/nodes_extra_on_<svc>.tsv        rows on <svc> absent on primary
#   gaps/missing_created_at_histo.tsv    second-bucket count of missing-node _created_at
#   gaps/missing_in_stream.tsv           elementIds found in XRANGE dump
#   gaps/missing_not_in_stream.tsv       elementIds never published
#   gaps/comm_divergence_on_<svc>.tsv    nodes whose `community` differs vs primary
#   redis/xpending_<group>.txt           applier pending entries per group
#   redis/stream_tail.txt                last 50 stream entries (for time alignment)
#   agent/switchover_window.log          filtered ha-agent log lines
#   SUMMARY.md                           human-readable verdict
#
# EXIT CODES
#   0  no gaps detected (all standbys match primary; script still ran)
#   1  gaps detected (see SUMMARY.md; this is the expected path when invoked
#      after a failed integrity check)
#   2  invalid arguments / missing prereqs

set -uo pipefail

# ------------------------------ args ------------------------------
WRITER_ID=""
OUT_DIR=""
ENV_FILE="${ENV_FILE:-docker/.env}"
AGENT_LOG_LINES="2000"
STREAM_KEY_ARG=""
QUIESCE_SECONDS="0"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --writer-id)        WRITER_ID="$2"; shift 2 ;;
        --out)              OUT_DIR="$2"; shift 2 ;;
        --env-file)         ENV_FILE="$2"; shift 2 ;;
        --agent-log-lines)  AGENT_LOG_LINES="$2"; shift 2 ;;
        --stream)           STREAM_KEY_ARG="$2"; shift 2 ;;
        --quiesce-seconds)  QUIESCE_SECONDS="$2"; shift 2 ;;
        -h|--help)          sed -n '2,75p' "$0"; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$WRITER_ID" ]]; then
    echo "ERROR: --writer-id is required (take it from the load test report)" >&2
    exit 2
fi

TS=$(date -u +%Y%m%dT%H%M%SZ)
if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="/tmp/cdc-gap-${TS}"
fi
mkdir -p "$OUT_DIR"

# ------------------------------ env ------------------------------
if [[ -f "$ENV_FILE" ]]; then
    set -a; . "$ENV_FILE"; set +a
fi

if [[ -z "${NEO4J_PASSWORD:-}" ]]; then
    echo "ERROR: NEO4J_PASSWORD not set (env or $ENV_FILE)" >&2
    exit 2
fi

: "${REDIS_HOST:=127.0.0.1}"
: "${REDIS_PORT:=6379}"
: "${NEO4J_SERVICES:=neo4j-primary neo4j-standby-1 neo4j-standby-2}"
: "${AGENT_CONTAINER:=ha-agent}"
: "${STREAM_KEY:=${STREAM_KEY_ARG:-neo4j:cdc:neo4j:changes}}"

RCLI=(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT")
[[ -n "${REDIS_PASSWORD:-}" ]] && RCLI+=(-a "$REDIS_PASSWORD" --no-auth-warning)

PRIMARY_SVC=""
for svc in $NEO4J_SERVICES; do
    if [[ "$svc" == "neo4j-primary" ]]; then PRIMARY_SVC="$svc"; fi
done
if [[ -z "$PRIMARY_SVC" ]]; then
    # pick the first service as reference if naming deviates
    PRIMARY_SVC=$(echo $NEO4J_SERVICES | awk '{print $1}')
fi

log() { printf '[%s] %s\n' "$(date -u +%T)" "$*" | tee -a "$OUT_DIR/run.log"; }

# ------------------------------ quiesce ------------------------------
# BUG-064 follow-up: CDC replication is async (Redis poll + batch apply), so
# sampling primary vs standby immediately after a load test often catches a
# tail of events mid-flight and flags them as gaps. Give the pipeline a bounded
# grace period to finish draining before we take the snapshot. The default 0
# keeps behaviour backward-compatible; pass --quiesce-seconds 5 from the load
# test harness or interactively when a false-positive is suspected.
if [[ "$QUIESCE_SECONDS" != "0" ]]; then
    printf '[%s] quiescing for %ss to let sync-applier drain Redis tail...\n' \
        "$(date -u +%T)" "$QUIESCE_SECONDS"
    sleep "$QUIESCE_SECONDS"
fi

# ------------------------------ step 1: dump TestNode sets ------------------------------
log "writer_id=$WRITER_ID  out=$OUT_DIR  primary=$PRIMARY_SVC  stream=$STREAM_KEY"

{
    echo "collected_at: $(date -u +%FT%TZ)"
    echo "writer_id: $WRITER_ID"
    echo "primary_service: $PRIMARY_SVC"
    echo "neo4j_services: $NEO4J_SERVICES"
    echo "stream_key: $STREAM_KEY"
    echo "redis: $REDIS_HOST:$REDIS_PORT"
} > "$OUT_DIR/meta.yml"

mkdir -p "$OUT_DIR/neo4j" "$OUT_DIR/gaps" "$OUT_DIR/redis" "$OUT_DIR/agent"

cypher_plain() {
    # cypher_plain <svc> <query>
    local svc="$1"; shift
    docker exec "$svc" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
        --format plain "$1" 2>&1
}

for svc in $NEO4J_SERVICES; do
    mkdir -p "$OUT_DIR/neo4j/$svc"
    log "dumping TestNode set from $svc"
    # NOTE: COALESCE guards nodes healed by NakedRelationshipHealer after the
    # trigger lost an update, so the diff below will still key by business id.
    cypher_plain "$svc" "
        MATCH (n:TestNode {writer:'$WRITER_ID'})
        RETURN n.id AS id,
               COALESCE(n._elementId, elementId(n)) AS eid,
               n._created_at AS created_at,
               n._updated_at AS updated_at,
               COALESCE(toString(n.community), '') AS community
        ORDER BY n.id ASC
    " > "$OUT_DIR/neo4j/$svc/testnodes.csv.raw"
    # Strip cypher-shell header + quotes, use TAB separator
    awk -F',' 'NR>1 {
        for (i=1; i<=NF; i++) {
            gsub(/^"|"$/, "", $i)
        }
        print $1"\t"$2"\t"$3"\t"$4"\t"$5
    }' "$OUT_DIR/neo4j/$svc/testnodes.csv.raw" | sort -u > "$OUT_DIR/neo4j/$svc/testnodes.tsv"

    log "dumping relationship set from $svc"
    cypher_plain "$svc" "
        MATCH (a:TestNode {writer:'$WRITER_ID'})-[r]->(b:TestNode {writer:'$WRITER_ID'})
        RETURN COALESCE(r._elementId, elementId(r)) AS eid,
               r._created_at AS created_at,
               r._updated_at AS updated_at,
               type(r) AS rel_type,
               a.id AS from_id,
               b.id AS to_id
        ORDER BY from_id, to_id ASC
    " > "$OUT_DIR/neo4j/$svc/rels.csv.raw"
    awk -F',' 'NR>1 {
        for (i=1; i<=NF; i++) { gsub(/^"|"$/, "", $i) }
        print $1"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6
    }' "$OUT_DIR/neo4j/$svc/rels.csv.raw" | sort -u > "$OUT_DIR/neo4j/$svc/rels.tsv"

    wc -l "$OUT_DIR/neo4j/$svc/testnodes.tsv" "$OUT_DIR/neo4j/$svc/rels.tsv" \
        | sed "s|$OUT_DIR/||" >> "$OUT_DIR/run.log"
done

# ------------------------------ step 2: set-diff per standby ------------------------------
PRIMARY_NODES="$OUT_DIR/neo4j/$PRIMARY_SVC/testnodes.tsv"
PRIMARY_RELS="$OUT_DIR/neo4j/$PRIMARY_SVC/rels.tsv"

if [[ ! -s "$PRIMARY_NODES" ]]; then
    log "ERROR: primary $PRIMARY_SVC has 0 TestNode rows for writer_id=$WRITER_ID"
    log "       Likely causes:"
    log "         - you ran the next load test with --clean-before-run (it wipes rows)"
    log "         - NakedRelationshipHealer / manual cleanup happened in between"
    log "       Remedy: rerun the load test WITHOUT --clean-before-run on the next"
    log "               iteration, or invoke this script BEFORE any subsequent run."
    log "       Proceeding anyway so we still capture stream/agent state."
fi

# Extract key (id for nodes; "from->to[type]" for rels) on each side.
cut -f1 "$PRIMARY_NODES" | sort -u > "$OUT_DIR/neo4j/$PRIMARY_SVC/node_keys.txt"
awk -F'\t' '{print $5"->"$6"["$4"]"}' "$PRIMARY_RELS" | sort -u > "$OUT_DIR/neo4j/$PRIMARY_SVC/rel_keys.txt"

TOTAL_GAPS=0
for svc in $NEO4J_SERVICES; do
    [[ "$svc" == "$PRIMARY_SVC" ]] && continue

    stb_nodes="$OUT_DIR/neo4j/$svc/testnodes.tsv"
    stb_rels="$OUT_DIR/neo4j/$svc/rels.tsv"

    cut -f1 "$stb_nodes" | sort -u > "$OUT_DIR/neo4j/$svc/node_keys.txt"
    awk -F'\t' '{print $5"->"$6"["$4"]"}' "$stb_rels" | sort -u > "$OUT_DIR/neo4j/$svc/rel_keys.txt"

    # Missing on this standby: present on primary, absent on standby
    comm -23 "$OUT_DIR/neo4j/$PRIMARY_SVC/node_keys.txt" \
             "$OUT_DIR/neo4j/$svc/node_keys.txt" \
        > "$OUT_DIR/gaps/nodes_missing_on_${svc}.ids.txt"
    # Extra on this standby: present on standby, absent on primary
    comm -13 "$OUT_DIR/neo4j/$PRIMARY_SVC/node_keys.txt" \
             "$OUT_DIR/neo4j/$svc/node_keys.txt" \
        > "$OUT_DIR/gaps/nodes_extra_on_${svc}.ids.txt"

    # Join missing ids back to primary rows (to learn _created_at + eid)
    join -t $'\t' -1 1 -2 1 \
        <(sort -u "$OUT_DIR/gaps/nodes_missing_on_${svc}.ids.txt") \
        <(sort -t $'\t' -k1,1 "$PRIMARY_NODES") \
        > "$OUT_DIR/gaps/nodes_missing_on_${svc}.tsv"

    join -t $'\t' -1 1 -2 1 \
        <(sort -u "$OUT_DIR/gaps/nodes_extra_on_${svc}.ids.txt") \
        <(sort -t $'\t' -k1,1 "$stb_nodes") \
        > "$OUT_DIR/gaps/nodes_extra_on_${svc}.tsv"

    n_miss=$(wc -l < "$OUT_DIR/gaps/nodes_missing_on_${svc}.tsv")
    n_extra=$(wc -l < "$OUT_DIR/gaps/nodes_extra_on_${svc}.tsv")
    log "  $svc: nodes_missing=$n_miss nodes_extra=$n_extra"
    TOTAL_GAPS=$((TOTAL_GAPS + n_miss + n_extra))

    # Rels diff
    comm -23 "$OUT_DIR/neo4j/$PRIMARY_SVC/rel_keys.txt" \
             "$OUT_DIR/neo4j/$svc/rel_keys.txt" \
        > "$OUT_DIR/gaps/rels_missing_on_${svc}.keys.txt"
    comm -13 "$OUT_DIR/neo4j/$PRIMARY_SVC/rel_keys.txt" \
             "$OUT_DIR/neo4j/$svc/rel_keys.txt" \
        > "$OUT_DIR/gaps/rels_extra_on_${svc}.keys.txt"
    r_miss=$(wc -l < "$OUT_DIR/gaps/rels_missing_on_${svc}.keys.txt")
    r_extra=$(wc -l < "$OUT_DIR/gaps/rels_extra_on_${svc}.keys.txt")
    log "  $svc: rels_missing=$r_miss rels_extra=$r_extra"
    TOTAL_GAPS=$((TOTAL_GAPS + r_miss + r_extra))

    # Enrich rels_missing keys with primary-side (eid, created_at, updated_at)
    # so step 4 can classify them as producer/consumer side.
    # rels.tsv schema: eid, created_at, updated_at, rel_type, from_id, to_id
    #   key = from_id"->"to_id"["rel_type"]"
    if [[ -s "$OUT_DIR/gaps/rels_missing_on_${svc}.keys.txt" ]]; then
        awk -F'\t' 'BEGIN{OFS="\t"} {
            key = $5"->"$6"["$4"]"
            print key, $1, $2, $3
        }' "$PRIMARY_RELS" | sort -t $'\t' -k1,1 -u \
            > "$OUT_DIR/gaps/_primary_rel_index.tsv"
        join -t $'\t' -1 1 -2 1 \
            <(sort -u "$OUT_DIR/gaps/rels_missing_on_${svc}.keys.txt") \
            "$OUT_DIR/gaps/_primary_rel_index.tsv" \
            > "$OUT_DIR/gaps/rels_missing_on_${svc}.tsv"
    else
        : > "$OUT_DIR/gaps/rels_missing_on_${svc}.tsv"
    fi

    # Community divergence (only for nodes present on both sides).
    # TSV schema: id, eid, created_at, updated_at, community  (5 cols)
    # After `join -1 1 -2 1` output columns are:
    #   $1=id  $2..$5=primary{eid,cts,uts,community}
    #          $6..$9=standby{eid,cts,uts,community}
    join -t $'\t' -1 1 -2 1 \
        <(sort -t $'\t' -k1,1 "$PRIMARY_NODES") \
        <(sort -t $'\t' -k1,1 "$stb_nodes") \
        | awk -F'\t' '$5 != $9 {print $1"\t"$5"\t"$9}' \
        > "$OUT_DIR/gaps/comm_divergence_on_${svc}.tsv"
    c_div=$(wc -l < "$OUT_DIR/gaps/comm_divergence_on_${svc}.tsv")
    log "  $svc: community_divergence=$c_div"
    TOTAL_GAPS=$((TOTAL_GAPS + c_div))
done

# ------------------------------ step 3: _created_at histogram of missing rows ------------------------------
log "building _created_at histogram for missing nodes (aggregated across standbys)"
: > "$OUT_DIR/gaps/missing_created_at_all.tsv"
for svc in $NEO4J_SERVICES; do
    [[ "$svc" == "$PRIMARY_SVC" ]] && continue
    f="$OUT_DIR/gaps/nodes_missing_on_${svc}.tsv"
    [[ -s "$f" ]] || continue
    awk -F'\t' -v s="$svc" '{print $3"\t"s"\t"$1"\t"$2}' "$f" \
        >> "$OUT_DIR/gaps/missing_created_at_all.tsv"
done

# Bucket by second
if [[ -s "$OUT_DIR/gaps/missing_created_at_all.tsv" ]]; then
    awk -F'\t' '{
        ts = $1 + 0          # created_at in ms
        bucket = int(ts/1000)
        count[bucket]++
    } END {
        for (b in count) print b"\t"count[b]
    }' "$OUT_DIR/gaps/missing_created_at_all.tsv" \
        | sort -n > "$OUT_DIR/gaps/missing_created_at_histo.tsv"

    # Also render a human-readable window
    awk -F'\t' 'BEGIN {min=0; max=0}
    {
        if (min==0 || $1<min) min=$1
        if ($1>max) max=$1
    } END {
        print "missing_window_first: "strftime("%FT%TZ", min, 1)" ("min")"
        print "missing_window_last:  "strftime("%FT%TZ", max, 1)" ("max")"
        print "missing_window_span_s: "(max-min)
    }' "$OUT_DIR/gaps/missing_created_at_histo.tsv" \
        > "$OUT_DIR/gaps/missing_window.yml"
else
    echo "(no missing nodes on any standby)" > "$OUT_DIR/gaps/missing_created_at_histo.tsv"
fi

# ------------------------------ step 4: check Redis stream for missing elementIds ------------------------------
log "dumping Redis stream head/tail + consumer groups"
"${RCLI[@]}" XLEN "$STREAM_KEY"    > "$OUT_DIR/redis/xlen.txt" 2>&1
"${RCLI[@]}" XINFO GROUPS "$STREAM_KEY" > "$OUT_DIR/redis/xinfo_groups.txt" 2>&1
"${RCLI[@]}" XREVRANGE "$STREAM_KEY" + - COUNT 50 > "$OUT_DIR/redis/stream_tail.txt" 2>&1

# XPENDING per consumer group
groups=$("${RCLI[@]}" XINFO GROUPS "$STREAM_KEY" 2>/dev/null \
    | awk '/^name$/{getline; print}')
for g in $groups; do
    safe_g=$(echo "$g" | tr '/:' '__')
    "${RCLI[@]}" XPENDING "$STREAM_KEY" "$g" > "$OUT_DIR/redis/xpending_${safe_g}.txt" 2>&1
    "${RCLI[@]}" XPENDING "$STREAM_KEY" "$g" - + 50 > "$OUT_DIR/redis/xpending_${safe_g}.detail.txt" 2>&1
done

# Build a single list of "missing" elementIds across all standbys (union).
{
    # Node-side missing eids (col 4 of missing_created_at_all.tsv).
    cat "$OUT_DIR/gaps/missing_created_at_all.tsv" 2>/dev/null \
        | awk -F'\t' '{print $2"\tNODE\t"$4}'
    # Rel-side missing eids from each standby's rels_missing tsv
    #   schema: key, eid, created_at, updated_at   → col 2 = eid
    for svc in $NEO4J_SERVICES; do
        [[ "$svc" == "$PRIMARY_SVC" ]] && continue
        f="$OUT_DIR/gaps/rels_missing_on_${svc}.tsv"
        [[ -s "$f" ]] || continue
        awk -F'\t' -v s="$svc" '{print s"\tREL\t"$2}' "$f"
    done
} | sort -u > "$OUT_DIR/gaps/missing_eids.labelled.tsv"

# Flat list for grep (column 3 = eid, dedup)
awk -F'\t' '{print $3}' "$OUT_DIR/gaps/missing_eids.labelled.tsv" \
    | sort -u > "$OUT_DIR/gaps/missing_eids.txt"

# Pre-create the stream-classification files so downstream wc/cat always work,
# even when there are zero missing elementIds.
: > "$OUT_DIR/gaps/missing_in_stream.tsv"
: > "$OUT_DIR/gaps/missing_not_in_stream.tsv"

n_miss_eids=$(wc -l < "$OUT_DIR/gaps/missing_eids.txt")
log "searching Redis XRANGE for $n_miss_eids missing elementIds (nodes + rels)"

if [[ "$n_miss_eids" -gt 0 ]]; then
    # If the full dump doesn't exist yet, pull a bounded one here.
    xlen=$("${RCLI[@]}" XLEN "$STREAM_KEY" 2>/dev/null | tr -d ' \r\n')
    if [[ -z "${xlen:-}" || ! "$xlen" =~ ^[0-9]+$ ]]; then xlen=0; fi
    log "stream xlen=$xlen"

    stream_dump="$OUT_DIR/redis/stream_full.txt"
    if [[ "$xlen" -gt 0 && "$xlen" -le 500000 ]]; then
        "${RCLI[@]}" XRANGE "$STREAM_KEY" - + > "$stream_dump" 2>&1
    elif [[ "$xlen" -gt 500000 ]]; then
        log "stream too large ($xlen); sampling head+tail 10k for eid search (coverage partial)"
        "${RCLI[@]}" XRANGE    "$STREAM_KEY" - + COUNT 10000 >  "$stream_dump" 2>&1
        "${RCLI[@]}" XREVRANGE "$STREAM_KEY" + - COUNT 10000 >> "$stream_dump" 2>&1
    else
        : > "$stream_dump"
    fi

    # Search elementIds. redis-cli XRANGE renders payloads as alternating
    # field/value lines; we grep on the raw bytes regardless. (Files are
    # already pre-created above so they exist even when n_miss_eids=0.)
    while read -r eid; do
        [[ -n "$eid" ]] || continue
        if grep -F -q -- "$eid" "$stream_dump"; then
            echo "$eid" >> "$OUT_DIR/gaps/missing_in_stream.tsv"
        else
            echo "$eid" >> "$OUT_DIR/gaps/missing_not_in_stream.tsv"
        fi
    done < "$OUT_DIR/gaps/missing_eids.txt"

    n_in=$(wc -l < "$OUT_DIR/gaps/missing_in_stream.tsv")
    n_out=$(wc -l < "$OUT_DIR/gaps/missing_not_in_stream.tsv")
    log "  elementIds in stream:   $n_in  (consumer-side gap if >0)"
    log "  elementIds NOT in stream: $n_out  (producer-side gap if >0)"
fi

# ------------------------------ step 4b: stream-level delete-event sanity + slot-reuse detect ------------------------------
#
# BUG-064 tripwire. The rel-delete APOC trigger historically failed silently
# (0 REL_DELETED across 228 NODE_DELETED in one load test). That leaves
# standbys with orphan edges whose primary counterpart has been cascaded
# away, and once Neo4j reuses the rel's internal slot the "old" _elementId
# comes back as a completely different edge, mis-classifying the consumer-side
# cause as producer-side. Detect both here.
if [[ -s "$OUT_DIR/redis/stream_full.txt" ]]; then
    stream_dump="$OUT_DIR/redis/stream_full.txt"
    n_node_del=$(grep -c 'NODE_DELETED'  "$stream_dump" 2>/dev/null || echo 0)
    n_rel_del=$(grep  -c 'REL_DELETED'   "$stream_dump" 2>/dev/null || echo 0)
    n_node_cre=$(grep -c 'NODE_CREATED'  "$stream_dump" 2>/dev/null || echo 0)
    n_rel_cre=$(grep  -c 'REL_CREATED'   "$stream_dump" 2>/dev/null || echo 0)
    {
        echo "stream_event_counts:"
        echo "  NODE_CREATED: $n_node_cre"
        echo "  NODE_DELETED: $n_node_del"
        echo "  REL_CREATED:  $n_rel_cre"
        echo "  REL_DELETED:  $n_rel_del"
    } > "$OUT_DIR/redis/stream_event_counts.yml"

    # BUG-064 tripwire: node-deletes > 0 but rel-deletes == 0 is physically
    # impossible under a workload that has any edges attached to deleted nodes
    # (the load test's rel_loop + delete_loop guarantees this).
    if [[ "$n_node_del" -gt 10 && "$n_rel_del" -eq 0 && "$n_rel_cre" -gt 0 ]]; then
        echo "  BUG064_SUSPECTED: true (NODE_DELETED > 0, REL_DELETED = 0 despite REL_CREATED > 0)" \
            >> "$OUT_DIR/redis/stream_event_counts.yml"
        log "WARN: NODE_DELETED=$n_node_del REL_DELETED=0 — cdc-capture-rel-deletes likely broken (BUG-064)"
    fi

    # Slot-reuse detection: for each missing rel's elementId, count distinct
    # _created_at values in the stream. Two or more ⇒ the same _elementId
    # has been carrying >1 logical relationship over time, which means the
    # sync-applier's _elementId-keyed MERGE/DELETE targeted the wrong lifecycle.
    # Only build this when we actually have missing rels (cheap lookup).
    : > "$OUT_DIR/gaps/slot_reuse.tsv"
    echo -e "eid\tdistinct_created_at\tfirst_created_at\tlast_created_at" \
        > "$OUT_DIR/gaps/slot_reuse.tsv.hdr"
    for svc in $NEO4J_SERVICES; do
        [[ "$svc" == "$PRIMARY_SVC" ]] && continue
        f="$OUT_DIR/gaps/rels_missing_on_${svc}.tsv"
        [[ -s "$f" ]] || continue
        # rels_missing.tsv schema: key, eid, created_at, updated_at (4 cols)
        awk -F'\t' '{print $2}' "$f" | sort -u >> "$OUT_DIR/gaps/slot_reuse_candidates.eids"
    done
    if [[ -s "$OUT_DIR/gaps/slot_reuse_candidates.eids" ]]; then
        sort -u "$OUT_DIR/gaps/slot_reuse_candidates.eids" \
            > "$OUT_DIR/gaps/slot_reuse_candidates.eids.uniq"
        while read -r eid; do
            [[ -n "$eid" ]] || continue
            # Extract all _created_at values for this elementId from the stream.
            # Payload shape: "elementId":"5:...:7", ... "_created_at":<ms>
            # We grep the lines containing the exact elementId, then pull the
            # _created_at from the same line. Dedup + count.
            cts_list=$(grep -F -- "\"elementId\":\"$eid\"" "$stream_dump" 2>/dev/null \
                | grep -oE '"_created_at":[0-9]+' \
                | awk -F':' '{print $2}' \
                | sort -u)
            if [[ -n "$cts_list" ]]; then
                n_distinct=$(echo "$cts_list" | wc -l | tr -d ' ')
                first=$(echo "$cts_list" | head -1)
                last=$(echo  "$cts_list" | tail -1)
                if [[ "$n_distinct" -ge 2 ]]; then
                    printf '%s\t%s\t%s\t%s\n' "$eid" "$n_distinct" "$first" "$last" \
                        >> "$OUT_DIR/gaps/slot_reuse.tsv"
                fi
            fi
        done < "$OUT_DIR/gaps/slot_reuse_candidates.eids.uniq"
        rm -f "$OUT_DIR/gaps/slot_reuse_candidates.eids" \
              "$OUT_DIR/gaps/slot_reuse_candidates.eids.uniq"
    fi
    n_reuse=$(wc -l < "$OUT_DIR/gaps/slot_reuse.tsv" 2>/dev/null | tr -d ' ' || echo 0)
    log "slot-reuse candidates among missing rel elementIds: $n_reuse"
fi

# ------------------------------ step 5: agent log around switchovers ------------------------------
log "capturing last $AGENT_LOG_LINES lines from $AGENT_CONTAINER"
docker logs --tail "$AGENT_LOG_LINES" "$AGENT_CONTAINER" \
    > "$OUT_DIR/agent/full_tail.log" 2>&1

# Filter for switchover-relevant events
grep -Ei 'switchover|failover|drainRelTriggerAfterAsync|InflightTxDrain|collector|applier|baseline|checkpoint|fencing|WARN|ERROR|Exception' \
    "$OUT_DIR/agent/full_tail.log" \
    > "$OUT_DIR/agent/switchover_window.log" 2>/dev/null || true

# Primary neo4j debug log tail (sometimes holds trigger errors)
docker exec "$PRIMARY_SVC" sh -c 'tail -n 500 /logs/debug.log 2>/dev/null' \
    > "$OUT_DIR/neo4j/$PRIMARY_SVC/debug_tail.log" 2>&1 || true

# ------------------------------ step 6: SUMMARY.md ------------------------------
log "writing SUMMARY.md"

SUMMARY="$OUT_DIR/SUMMARY.md"
{
    echo "# CDC Gap Diagnosis — $WRITER_ID"
    echo
    echo "- collected_at: $(date -u +%FT%TZ)"
    echo "- primary: \`$PRIMARY_SVC\`"
    echo "- stream:  \`$STREAM_KEY\`"
    echo
    echo "## Per-node counts"
    echo
    printf '| service | testnodes | rels |\n'
    printf '|---|---|---|\n'
    for svc in $NEO4J_SERVICES; do
        tn=$(wc -l < "$OUT_DIR/neo4j/$svc/testnodes.tsv")
        rl=$(wc -l < "$OUT_DIR/neo4j/$svc/rels.tsv")
        printf '| %s | %s | %s |\n' "$svc" "$tn" "$rl"
    done
    echo
    echo "## Standby divergence vs primary"
    echo
    printf '| standby | nodes_missing | nodes_extra | rels_missing | rels_extra | community_div |\n'
    printf '|---|---|---|---|---|---|\n'
    for svc in $NEO4J_SERVICES; do
        [[ "$svc" == "$PRIMARY_SVC" ]] && continue
        nm=$(wc -l < "$OUT_DIR/gaps/nodes_missing_on_${svc}.tsv" 2>/dev/null || echo 0)
        ne=$(wc -l < "$OUT_DIR/gaps/nodes_extra_on_${svc}.tsv"   2>/dev/null || echo 0)
        rm=$(wc -l < "$OUT_DIR/gaps/rels_missing_on_${svc}.keys.txt" 2>/dev/null || echo 0)
        re=$(wc -l < "$OUT_DIR/gaps/rels_extra_on_${svc}.keys.txt"   2>/dev/null || echo 0)
        cd=$(wc -l < "$OUT_DIR/gaps/comm_divergence_on_${svc}.tsv"   2>/dev/null || echo 0)
        printf '| %s | %s | %s | %s | %s | %s |\n' "$svc" "$nm" "$ne" "$rm" "$re" "$cd"
    done
    echo
    if [[ -s "$OUT_DIR/gaps/missing_window.yml" ]]; then
        echo "## Missing-node _created_at window (across standbys)"
        echo
        echo '```'
        cat "$OUT_DIR/gaps/missing_window.yml"
        echo '```'
        echo
        echo "### Per-second histogram (top 20 seconds)"
        echo
        echo '```'
        sort -t $'\t' -k2,2 -n -r "$OUT_DIR/gaps/missing_created_at_histo.tsv" \
            | head -20 \
            | awk -F'\t' '{printf "%s (%s)\t%s\n", strftime("%FT%TZ", $1, 1), $1, $2}'
        echo '```'
        echo
    fi

    n_in=$(wc -l < "$OUT_DIR/gaps/missing_in_stream.tsv" 2>/dev/null || echo 0)
    n_out=$(wc -l < "$OUT_DIR/gaps/missing_not_in_stream.tsv" 2>/dev/null || echo 0)
    n_eids=$(wc -l < "$OUT_DIR/gaps/missing_eids.txt" 2>/dev/null || echo 0)

    if [[ -s "$OUT_DIR/redis/stream_event_counts.yml" ]]; then
        echo "## Stream event counts"
        echo
        echo '```'
        cat "$OUT_DIR/redis/stream_event_counts.yml"
        echo '```'
        echo
        if grep -q 'BUG064_SUSPECTED: true' "$OUT_DIR/redis/stream_event_counts.yml"; then
            echo "**BUG-064 suspected**: NODE_DELETED > 0 but REL_DELETED = 0"
            echo "despite REL_CREATED > 0. The \`cdc-capture-rel-deletes\` APOC"
            echo "trigger is silently failing to produce \`_CDCDeleteEvent\` rows."
            echo "Verify trigger body does NOT use \`apoc.map.fromPairs\` over"
            echo "\`\$removedRelationshipProperties\` (APOC 5.x compatibility bug)."
            echo "Rebuild ha-agent with the BUG-064 fix and confirm REL_DELETED > 0."
            echo
        fi
    fi

    if [[ -s "$OUT_DIR/gaps/slot_reuse.tsv" ]]; then
        n_reuse=$(wc -l < "$OUT_DIR/gaps/slot_reuse.tsv")
        echo "## Slot-reuse candidates (rel elementIds carrying >1 lifecycle)"
        echo
        echo "$n_reuse missing relationship elementId(s) appear in the Redis"
        echo "stream with >1 distinct \`_created_at\` value — Neo4j has reused"
        echo "their internal slot across a delete+create boundary. The applier's"
        echo "\`_elementId\`-keyed MERGE/DELETE cannot distinguish lifecycles on"
        echo "its own, so at least one of the lifecycles was mis-applied on the"
        echo "standby. Always pair with the NODE_DELETED / REL_DELETED count"
        echo "sanity check above; slot reuse is harmless ONLY if REL_DELETED"
        echo "events for the earlier lifecycle made it through."
        echo
        echo '```'
        { echo -e "eid\tdistinct_created_at\tfirst_created_at\tlast_created_at";
          head -20 "$OUT_DIR/gaps/slot_reuse.tsv"; } | column -t -s $'\t'
        echo '```'
        echo
    fi

    echo "## Pipeline verdict"
    echo
    if [[ "$n_eids" -eq 0 ]]; then
        echo "- No missing elementIds to classify (either no gaps, or primary had already been cleaned)."
    else
        pct_in=$(awk -v a="$n_in" -v b="$n_eids" 'BEGIN{printf "%.1f", (b>0? 100.0*a/b:0)}')
        pct_out=$(awk -v a="$n_out" -v b="$n_eids" 'BEGIN{printf "%.1f", (b>0? 100.0*a/b:0)}')
        printf -- '- missing elementIds searched: **%s**\n' "$n_eids"
        printf -- '- found in Redis stream:       **%s** (%s%%) → consumer-side gap (check applier / XPENDING)\n' "$n_in" "$pct_in"
        printf -- '- NOT in Redis stream:         **%s** (%s%%) → producer-side gap (BUG-061 class: afterAsync tail / collector baseline)\n' "$n_out" "$pct_out"
        echo
        if [[ "$n_out" -gt 0 && "$n_in" -eq 0 ]]; then
            echo "**Verdict: producer-side drop.** The old primary never published these events."
            echo "Inspect \`agent/switchover_window.log\` around the missing_window timestamps"
            echo "for \`drainRelTriggerAfterAsync\`, \`InflightTxDrain\`, or collector baseline logs."
        elif [[ "$n_in" -gt 0 && "$n_out" -eq 0 ]]; then
            echo "**Verdict: consumer-side drop.** Events reached Redis but the applier"
            echo "did not materialize them on the standby. Inspect \`redis/xpending_*.txt\`"
            echo "and \`agent/switchover_window.log\` for applier errors."
        elif [[ "$n_in" -gt 0 && "$n_out" -gt 0 ]]; then
            echo "**Verdict: mixed.** Some events were never published AND some were"
            echo "published but not applied. Treat both paths."
        fi
    fi
    echo
    echo "## Artifacts"
    echo
    echo "- full tree: \`$OUT_DIR\`"
    echo "- per-standby gap files: \`gaps/nodes_missing_on_*.tsv\`, \`gaps/rels_*\`, \`gaps/comm_divergence_on_*.tsv\`"
    echo "- stream dump: \`redis/stream_full.txt\` (or head/tail 10k samples if stream was too large)"
    echo "- agent window: \`agent/switchover_window.log\`"
} > "$SUMMARY"

# ------------------------------ done ------------------------------
cat "$SUMMARY"
log "done: $OUT_DIR"

if [[ "$TOTAL_GAPS" -gt 0 ]]; then
    exit 1
fi
exit 0
