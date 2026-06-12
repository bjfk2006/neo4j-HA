#!/usr/bin/env bash
# Detect the last-known Neo4j master when Redis persistence is lost.
#
# Context: HA Agent's cluster state (node-registry, fencing-token, checkpoints) lives in
# Redis. If Redis data is wiped and has to be rebuilt, the information about "who was the
# master at shutdown" is gone. Cold-starting Agent from `ha-agent.yml`'s stale `role` field
# risks promoting a data-stale node, causing silent data rollback.
#
# This script queries each Neo4j node directly for three independent signals:
#   (1) max _updated_at across nodes and relationships (data recency)
#   (2) _CDCDeleteEvent transit node count and latest ts (only produced on master)
#   (3) cdc-* APOC triggers installed (only the master carries them after switchover)
#
# Usage:
#   NEO4J_PASSWORD=<pw> bash scripts/deploy/detect-last-master.sh
#
# Optionally override the host list (Docker container names by default):
#   HOSTS="neo4j-a neo4j-b neo4j-c" NEO4J_PASSWORD=<pw> bash ...
#
# Exit codes:
#   0  — a single candidate unambiguously identified
#   1  — no candidate identifiable (all signals empty)
#   2  — conflicting signals, manual inspection required

set -euo pipefail

HOSTS="${HOSTS:-neo4j-primary neo4j-standby-1 neo4j-standby-2}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-}"
DATABASE="${DATABASE:-neo4j}"

[[ -n "${NEO4J_PASSWORD}" ]] || {
  echo "[ERROR] NEO4J_PASSWORD not set" >&2
  exit 3
}

cyphershell() {
  local host="$1"; shift
  docker exec "${host}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
    -d "${DATABASE}" --format plain "$@" 2>&1 \
    | grep -v '^WARNING:' \
    | grep -v '^$'
}

# Return a single numeric value from a one-row Cypher query.
scalar() {
  local host="$1" query="$2"
  local out
  out="$(cyphershell "${host}" <<<"${query}" | tail -n 1 | tr -d ' "')"
  [[ "${out}" =~ ^[0-9]+$ ]] && echo "${out}" || echo "0"
}

printf '\n%-22s %-20s %-20s %-14s %-10s\n' \
  "node" "max_node_ts" "max_rel_ts" "delete_events" "triggers"
printf '%-22s %-20s %-20s %-14s %-10s\n' \
  "----" "-----------" "----------" "-------------" "--------"

declare -A NODE_MAX_TS
declare -A DELETE_COUNT
declare -A TRIGGER_COUNT

MAX_TS_SEEN=0
for host in ${HOSTS}; do
  node_ts="$(scalar "${host}" \
    "MATCH (n) WHERE n._updated_at IS NOT NULL RETURN coalesce(max(n._updated_at), 0);")"
  rel_ts="$(scalar "${host}" \
    "MATCH ()-[r]->() WHERE r._updated_at IS NOT NULL RETURN coalesce(max(r._updated_at), 0);")"
  del_cnt="$(scalar "${host}" "MATCH (e:_CDCDeleteEvent) RETURN count(e);")"

  # Trigger listing needs special handling — count lines that start with "cdc-"
  trig_cnt="$(cyphershell "${host}" \
    <<<"CALL apoc.trigger.list() YIELD name WHERE name STARTS WITH 'cdc-' RETURN name;" \
    | grep -c '^"cdc-' || true)"

  # Combined data freshness: max of node and relationship timestamps
  local_max=$(( node_ts > rel_ts ? node_ts : rel_ts ))
  NODE_MAX_TS[$host]=${local_max}
  DELETE_COUNT[$host]=${del_cnt}
  TRIGGER_COUNT[$host]=${trig_cnt}

  if (( local_max > MAX_TS_SEEN )); then
    MAX_TS_SEEN=${local_max}
  fi

  printf '%-22s %-20s %-20s %-14s %-10s\n' \
    "${host}" "${node_ts}" "${rel_ts}" "${del_cnt}" "${trig_cnt}"
done

echo

# ---------- Decision ----------

# Candidates: nodes whose local max_ts equals the overall max (within 1ms tolerance).
FRESHEST=()
for host in ${HOSTS}; do
  if (( ${NODE_MAX_TS[$host]} >= MAX_TS_SEEN && MAX_TS_SEEN > 0 )); then
    FRESHEST+=("${host}")
  fi
done

WITH_TRIGGERS=()
for host in ${HOSTS}; do
  if (( ${TRIGGER_COUNT[$host]} > 0 )); then
    WITH_TRIGGERS+=("${host}")
  fi
done

WITH_DELETES=()
for host in ${HOSTS}; do
  if (( ${DELETE_COUNT[$host]} > 0 )); then
    WITH_DELETES+=("${host}")
  fi
done

echo "Signal summary:"
echo "  Freshest (data-recency):   ${FRESHEST[*]:-<none>}"
echo "  With cdc-* triggers:       ${WITH_TRIGGERS[*]:-<none>}"
echo "  With _CDCDeleteEvent rows: ${WITH_DELETES[*]:-<none>}"
echo

# Strongest signal: cdc-* triggers. If exactly one node has them, that's the answer.
if (( ${#WITH_TRIGGERS[@]} == 1 )); then
  winner="${WITH_TRIGGERS[0]}"
  fresh_ok="no"
  for h in "${FRESHEST[@]}"; do
    [[ "${h}" == "${winner}" ]] && fresh_ok="yes"
  done
  if [[ "${fresh_ok}" == "yes" ]]; then
    echo "VERDICT: ${winner} is the last-known master (cdc-* triggers installed AND data is freshest)."
    exit 0
  else
    echo "CONFLICT: ${winner} has cdc-* triggers but is NOT the freshest node." >&2
    echo "  Freshest is ${FRESHEST[*]}. This typically means switchover never completed its" >&2
    echo "  trigger-uninstall step on the actual last master. Investigate manually." >&2
    exit 2
  fi
fi

if (( ${#WITH_TRIGGERS[@]} > 1 )); then
  echo "CONFLICT: cdc-* triggers present on multiple nodes: ${WITH_TRIGGERS[*]}" >&2
  echo "  A previous switchover failed to uninstall triggers on the old master." >&2
  echo "  Use the freshest node (${FRESHEST[*]}) as the new master, then manually drop" >&2
  echo "  cdc-* triggers on the other node(s):" >&2
  echo >&2
  echo "    CALL apoc.trigger.drop('neo4j', 'cdc-timestamp');" >&2
  echo "    CALL apoc.trigger.drop('neo4j', 'cdc-capture-node-deletes');" >&2
  echo "    CALL apoc.trigger.drop('neo4j', 'cdc-capture-rel-deletes');" >&2
  echo "    MATCH (e:_CDCDeleteEvent) DETACH DELETE e;" >&2
  exit 2
fi

# No triggers anywhere. Fall back to data-freshness + delete events.
if (( ${#WITH_TRIGGERS[@]} == 0 )); then
  if (( ${#FRESHEST[@]} == 1 )); then
    winner="${FRESHEST[0]}"
    echo "VERDICT: ${winner} is the last-known master (no triggers anywhere,"
    echo "         but it alone has the freshest data)."
    echo "         Note: cdc-* triggers will be (re-)installed when HA Agent starts."
    exit 0
  fi
  if (( ${#WITH_DELETES[@]} == 1 )); then
    winner="${WITH_DELETES[0]}"
    fresh_ok="no"
    for h in "${FRESHEST[@]}"; do
      [[ "${h}" == "${winner}" ]] && fresh_ok="yes"
    done
    if [[ "${fresh_ok}" == "yes" ]]; then
      echo "VERDICT: ${winner} is the last-known master (tied on max_ts, but it alone"
      echo "         has residual _CDCDeleteEvent transit nodes — a master-only artefact)."
      exit 0
    fi
  fi
  echo "AMBIGUOUS: Multiple nodes share the freshest timestamp (${FRESHEST[*]})." >&2
  echo "  No single tiebreaker found. Recommended next steps:" >&2
  echo "  1. Compare node/relationship counts — they should match exactly on all" >&2
  echo "     candidates if the cluster was in a steady state at shutdown." >&2
  echo "  2. Compare recent write timestamps per label (application-specific)." >&2
  echo "  3. If you must pick one, choose any freshest node; data equivalence is" >&2
  echo "     confirmed by identical max_ts." >&2
  exit 2
fi

echo "UNEXPECTED: fell through all decision branches." >&2
exit 1
