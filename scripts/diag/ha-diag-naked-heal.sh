#!/usr/bin/env bash
# ha-diag-naked-heal.sh — repair "naked" relationships/nodes on the primary
# and propagate the fix to all standbys via CDC.
#
# WHAT IS A "NAKED" ENTITY:
#   In this HA cluster, every business node and relationship must carry the
#   CDC-stable identity fields:
#     - _elementId      (cluster-stable id)
#     - _updated_at     (CDC keyset cursor)
#     - _created_at     (for NakedRelationshipHealer fast-path)
#     - _type           (rel only; needed by cdc-capture-rel-deletes)
#     - _labels         (node only; needed by cdc-capture-node-deletes)
#   These are stamped by the APOC triggers installed via ApocTriggerInstaller.
#   When the trigger drops a task (BUG-062), or when client code violates the
#   R0 contract via `SET r += props` / `SET r = properties(r')` and transitively
#   moves stale `_*` fields onto a new relationship, the resulting entity can
#   end up with one or more `_*` fields NULL.
#
#   Consequences:
#     - Naked rel/node with `_updated_at IS NULL` → invisible to CDC, never
#       replicated to standby.
#     - Rel with `_type IS NULL` → on delete, `cdc-capture-rel-deletes` skips
#       it (it scans `$removedRelationshipProperties["_type"]`), so the delete
#       is invisible to standby → standby permanently keeps an orphan.
#
# WHAT THIS SCRIPT DOES:
#   1. Auto-discovers all user labels and relationship types
#      (excluding `_`-prefixed HA internals) on the primary.
#   2. Counts naked rels/nodes per type/label.
#   3. In batched transactions, SETs missing `_*` fields back to a sane value
#      (using coalesce so already-set values are not overwritten), and
#      force-bumps `_updated_at = timestamp()` so the change is visible to the
#      CDC keyset poll.
#   4. Sleeps to let the CDC collector + sync-applier propagate the fix to
#      every standby.
#   5. Verifies post-heal state: primary should have 0 naked, standbys should
#      have 0 naked, and label/relType counts should match across all nodes.
#
# REQUIRED ENV:
#   NEO4J_USER, NEO4J_PASSWORD     credentials shared by all nodes
#   NEO4J_PRIMARY                  container name of the primary (e.g. neo4j-primary)
#
# OPTIONAL ENV:
#   NEO4J_STANDBYS                 space-separated standby container names
#                                  (e.g. "neo4j-standby-1 neo4j-standby-2")
#   CDC_WAIT_SEC=60                seconds to wait for CDC to drain after heal
#   BATCH_SIZE=5000                max entities mutated per inner transaction
#   BOLT_PORT=7687                 bolt port if non-standard
#   NEO4J_DATABASE=neo4j           Neo4j database name if non-default
#   INCLUDE_LABELS=""              comma-separated; empty → all user labels
#   EXCLUDE_LABELS=""              comma-separated; in addition to `_*` filter
#   INCLUDE_REL_TYPES=""           comma-separated; empty → all user rel types
#   EXCLUDE_REL_TYPES=""           comma-separated; in addition to `_*` filter
#   CREATED_AT_FALLBACK_FIELD="createdAt"
#                                  business-level createdAt field name written
#                                  by the client (HA contract §R1); used as a
#                                  fallback for `_created_at` if absent
#   DRY_RUN=0                      1 → only report what would be healed
#
# EXIT CODES:
#   0   fully clean (no naked rel/node anywhere)
#   1   bad input (missing env)
#   2   heal ran but residual naked still observed (CDC catching up or
#       standby has its own orphans not present on primary)
#   3   primary unreachable
#
# SAFETY:
#   - Writes are limited to primary; standbys are read-only.
#   - Uses `coalesce` so `_elementId` / `_created_at` / `_type` / `_labels`
#     are never overwritten if already set.
#   - The only field force-set is `_updated_at = timestamp()` — needed so the
#     CDC keyset poll sees the change. Triggers do not chain-fire on rel SET
#     (BUG-059 omits `$assignedRelationshipProperties`).
#   - Each batch is its own implicit transaction; partial progress is safe and
#     idempotent. Re-running the script will pick up wherever it left off.

set -uo pipefail

####################################################################
# Input validation
####################################################################
: "${NEO4J_USER:?must set NEO4J_USER}"
: "${NEO4J_PASSWORD:?must set NEO4J_PASSWORD}"
: "${NEO4J_PRIMARY:?must set NEO4J_PRIMARY (container name)}"

NEO4J_STANDBYS="${NEO4J_STANDBYS:-}"
CDC_WAIT_SEC="${CDC_WAIT_SEC:-60}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
BOLT_PORT="${BOLT_PORT:-7687}"
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"
INCLUDE_LABELS="${INCLUDE_LABELS:-}"
EXCLUDE_LABELS="${EXCLUDE_LABELS:-}"
INCLUDE_REL_TYPES="${INCLUDE_REL_TYPES:-}"
EXCLUDE_REL_TYPES="${EXCLUDE_REL_TYPES:-}"
CREATED_AT_FALLBACK_FIELD="${CREATED_AT_FALLBACK_FIELD:-createdAt}"
DRY_RUN="${DRY_RUN:-0}"

####################################################################
# Helpers
####################################################################
cy() {
  # cy <container> <cypher>
  docker exec "$1" cypher-shell \
    -a "bolt://localhost:${BOLT_PORT}" \
    -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" \
    -d "$NEO4J_DATABASE" --format plain "$2"
}
scalar()   { cy "$1" "$2" | tail -n +2 | head -n 1 | tr -d ' "'; }
list_col() { cy "$1" "$2" | tail -n +2 | tr -d ' "' | sed '/^$/d'; }
hr()       { printf '============================================================\n'; }
sec()      { printf '\n'; hr; printf '# %s\n' "$1"; hr; }

csv_to_quoted_list() {
  # "A,B,C" -> "'A','B','C'"
  local IFS=','; local arr=($1); local out=""
  for x in "${arr[@]}"; do
    [ -z "$x" ] && continue
    out+="'${x}',"
  done
  echo "${out%,}"
}

####################################################################
sec "Step 0  Connectivity check"
####################################################################
if ! cy "$NEO4J_PRIMARY" "RETURN 1 AS ok" >/dev/null 2>&1; then
  echo "ERR: primary container '${NEO4J_PRIMARY}' unreachable or bolt rejected"
  exit 3
fi
echo "primary OK: ${NEO4J_PRIMARY}"
for c in $NEO4J_STANDBYS; do
  if cy "$c" "RETURN 1" >/dev/null 2>&1; then
    echo "standby OK: ${c}"
  else
    echo "WARN standby unreachable: ${c} (post-verify will skip this node)"
  fi
done

####################################################################
sec "Step 1  Auto-discover labels + relationship types"
####################################################################
if [ -n "$INCLUDE_LABELS" ]; then
  LBL_FILTER="WHERE label IN [$(csv_to_quoted_list "$INCLUDE_LABELS")]"
else
  LBL_FILTER="WHERE NOT label STARTS WITH '_'"
fi
if [ -n "$EXCLUDE_LABELS" ]; then
  LBL_FILTER+=" AND NOT label IN [$(csv_to_quoted_list "$EXCLUDE_LABELS")]"
fi
labels=$(list_col "$NEO4J_PRIMARY" "
  CALL db.labels() YIELD label
  ${LBL_FILTER}
  RETURN label ORDER BY label")
echo "labels in scope:"
echo "$labels" | sed 's/^/  /'

if [ -n "$INCLUDE_REL_TYPES" ]; then
  REL_FILTER="WHERE relationshipType IN [$(csv_to_quoted_list "$INCLUDE_REL_TYPES")]"
else
  REL_FILTER="WHERE NOT relationshipType STARTS WITH '_'"
fi
if [ -n "$EXCLUDE_REL_TYPES" ]; then
  REL_FILTER+=" AND NOT relationshipType IN [$(csv_to_quoted_list "$EXCLUDE_REL_TYPES")]"
fi
reltypes=$(list_col "$NEO4J_PRIMARY" "
  CALL db.relationshipTypes() YIELD relationshipType
  ${REL_FILTER}
  RETURN relationshipType ORDER BY relationshipType")
echo "relationship types in scope:"
echo "$reltypes" | sed 's/^/  /'

# Serialize label/type lists as Cypher literal `['A','B',...]`
labels_cy=""
for l in $labels; do labels_cy+="'$l',"; done
labels_cy="[${labels_cy%,}]"
reltypes_cy=""
for t in $reltypes; do reltypes_cy+="'$t',"; done
reltypes_cy="[${reltypes_cy%,}]"

if [ -z "$labels" ] && [ -z "$reltypes" ]; then
  echo "No labels or relationship types to process; exiting."
  exit 0
fi

####################################################################
sec "Step 2  Pre-heal diagnostic (primary)"
####################################################################
echo "-- naked rel breakdown by type --"
cy "$NEO4J_PRIMARY" "
  MATCH ()-[r]->() WHERE type(r) IN ${reltypes_cy}
    AND (r._elementId IS NULL OR r._updated_at IS NULL
      OR r._created_at IS NULL OR r._type IS NULL)
  RETURN type(r)                                                AS rt,
         sum(CASE WHEN r._elementId  IS NULL THEN 1 ELSE 0 END) AS no_eid,
         sum(CASE WHEN r._updated_at IS NULL THEN 1 ELSE 0 END) AS no_uts,
         sum(CASE WHEN r._created_at IS NULL THEN 1 ELSE 0 END) AS no_cts,
         sum(CASE WHEN r._type       IS NULL THEN 1 ELSE 0 END) AS no_type,
         count(r)                                               AS total
  ORDER BY rt"

echo
echo "-- naked node breakdown by label --"
cy "$NEO4J_PRIMARY" "
  MATCH (n) WHERE any(l IN labels(n) WHERE l IN ${labels_cy})
  WITH n WHERE n._elementId IS NULL OR n._updated_at IS NULL OR n._labels IS NULL
  RETURN labels(n)[0]                                           AS lbl,
         sum(CASE WHEN n._elementId  IS NULL THEN 1 ELSE 0 END) AS no_eid,
         sum(CASE WHEN n._updated_at IS NULL THEN 1 ELSE 0 END) AS no_uts,
         sum(CASE WHEN n._labels     IS NULL THEN 1 ELSE 0 END) AS no_labels,
         count(n)                                               AS total
  ORDER BY lbl"

if [ "$DRY_RUN" = "1" ]; then
  echo
  echo ">> DRY_RUN=1: stopping after diagnostic, no writes performed."
  exit 0
fi

####################################################################
sec "Step 3  Heal naked rels on primary (batch=${BATCH_SIZE})"
####################################################################
total_rel_healed=0; batch_no=0
while true; do
  batch_no=$((batch_no+1))
  healed=$(scalar "$NEO4J_PRIMARY" "
    MATCH ()-[r]->() WHERE type(r) IN ${reltypes_cy}
      AND (r._elementId IS NULL OR r._updated_at IS NULL
        OR r._created_at IS NULL OR r._type IS NULL)
    WITH r LIMIT ${BATCH_SIZE}
    SET r._elementId  = coalesce(r._elementId,  elementId(r)),
        r._created_at = coalesce(r._created_at, r.\`${CREATED_AT_FALLBACK_FIELD}\`, timestamp()),
        r._type       = coalesce(r._type,       type(r)),
        r._updated_at = timestamp()
    RETURN count(r) AS healed")
  healed=${healed:-0}
  [ "$healed" = "0" ] && break
  total_rel_healed=$((total_rel_healed + healed))
  printf '  batch %02d  healed=%-6s  cumulative=%s\n' "$batch_no" "$healed" "$total_rel_healed"
done
echo "  → done, total rel healed = ${total_rel_healed}"

####################################################################
sec "Step 4  Heal naked nodes on primary (batch=${BATCH_SIZE})"
####################################################################
total_node_healed=0; batch_no=0
while true; do
  batch_no=$((batch_no+1))
  healed=$(scalar "$NEO4J_PRIMARY" "
    MATCH (n) WHERE any(l IN labels(n) WHERE l IN ${labels_cy})
    WITH n WHERE n._elementId IS NULL OR n._updated_at IS NULL OR n._labels IS NULL
    WITH n LIMIT ${BATCH_SIZE}
    SET n._elementId  = coalesce(n._elementId, elementId(n)),
        n._labels     = coalesce(n._labels,    apoc.convert.toJson(labels(n))),
        n._updated_at = timestamp()
    RETURN count(n) AS healed")
  healed=${healed:-0}
  [ "$healed" = "0" ] && break
  total_node_healed=$((total_node_healed + healed))
  printf '  batch %02d  healed=%-6s  cumulative=%s\n' "$batch_no" "$healed" "$total_node_healed"
done
echo "  → done, total node healed = ${total_node_healed}"

####################################################################
sec "Step 5  Wait ${CDC_WAIT_SEC}s for CDC to propagate heal to standbys"
####################################################################
if [ -z "$NEO4J_STANDBYS" ]; then
  echo "  (NEO4J_STANDBYS empty, skipping wait)"
else
  for i in $(seq 1 "$CDC_WAIT_SEC"); do
    printf '  %ds / %ds\r' "$i" "$CDC_WAIT_SEC"
    sleep 1
  done
  echo
fi

####################################################################
sec "Step 6  Post-heal residue per node"
####################################################################
fail=0
for c in $NEO4J_PRIMARY $NEO4J_STANDBYS; do
  cy "$c" "RETURN 1" >/dev/null 2>&1 || { echo "  $c  (skipped, unreachable)"; continue; }
  rel_left=$(scalar "$c" "
    MATCH ()-[r]->() WHERE type(r) IN ${reltypes_cy}
      AND (r._elementId IS NULL OR r._updated_at IS NULL
        OR r._created_at IS NULL OR r._type IS NULL)
    RETURN count(r) AS n")
  node_left=$(scalar "$c" "
    MATCH (n) WHERE any(l IN labels(n) WHERE l IN ${labels_cy})
    WITH n WHERE n._elementId IS NULL OR n._updated_at IS NULL OR n._labels IS NULL
    RETURN count(n) AS n")
  rel_left=${rel_left:-?}; node_left=${node_left:-?}
  status="OK"
  if [ "$rel_left" != "0" ] || [ "$node_left" != "0" ]; then
    status="NOT CLEAN"
    fail=1
  fi
  printf '  %-30s naked_rel=%-6s naked_node=%-6s  %s\n' \
    "$c" "$rel_left" "$node_left" "$status"
done

####################################################################
sec "Step 7  Count parity across all reachable nodes"
####################################################################
for c in $NEO4J_PRIMARY $NEO4J_STANDBYS; do
  cy "$c" "RETURN 1" >/dev/null 2>&1 || continue
  echo "-- ${c}: labels --"
  cy "$c" "
    MATCH (n) WHERE any(l IN labels(n) WHERE l IN ${labels_cy})
    UNWIND labels(n) AS lbl
    WITH lbl WHERE lbl IN ${labels_cy}
    RETURN lbl, count(*) AS cnt ORDER BY lbl"
  echo "-- ${c}: relationship types --"
  cy "$c" "
    MATCH ()-[r]->() WHERE type(r) IN ${reltypes_cy}
    RETURN type(r) AS rt, count(r) AS cnt ORDER BY rt"
done

####################################################################
sec "Summary"
####################################################################
echo "  scope:"
echo "    labels    = $(echo $labels | tr '\n' ' ')"
echo "    rel types = $(echo $reltypes | tr '\n' ' ')"
echo "  heal totals:"
echo "    rel healed  = ${total_rel_healed}"
echo "    node healed = ${total_node_healed}"
echo
if [ "$fail" = "0" ]; then
  echo "  OK   no naked rel/node remaining on any reachable node."
  echo "       If Step 7 label/relType counts match across primary and"
  echo "       standbys, the cluster is fully consistent."
  exit 0
else
  echo "  WARN naked residue remaining. Possible causes:"
  echo "       a) CDC still draining → re-run with larger CDC_WAIT_SEC."
  echo "       b) Standby has orphans NOT present on primary (delete events"
  echo "          lost in the past, before client-side R0 fix). Use the"
  echo "          standby orphan cleanup procedure documented in section"
  echo "          4.6.4.2 of ha-agent-cluster-operations.md."
  echo "       c) Live writes during heal created new naked rel/node"
  echo "          (low probability; re-run after write storm subsides)."
  exit 2
fi
