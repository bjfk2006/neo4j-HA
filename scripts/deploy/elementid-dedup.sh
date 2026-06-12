#!/usr/bin/env bash
# elementid-dedup.sh — BUG-083 一次性清理：移除 primary 上同 `_elementId` 属性
# 撞值的多余节点，并触发 fullsync 把清理结果同步到 standby。
#
# 背景:
#   `cdc-timestamp-created` trigger 在节点 CREATE 时按
#   `coalesce(n._elementId, elementId(n))` 写 `_elementId` 属性。
#   PostSwitchoverReconciler / SyncApplier 在新主上 MERGE 桩节点时会
#   主动 SET `_elementId = $oldPrimaryEid`。两者本身都对，但两者结合
#   后存在罕见碰撞窗口：reconciler 桩节点的 prop `_elementId` 写下后，
#   新主独立的内部 id 池如果稍后给客户端节点分配到了同样的内部 id，
#   trigger 就会把同一字符串再次写到 prop，导致两条活节点共享 prop
#   `_elementId`。standby applier 用 prop 做 MERGE 主键，后到事件覆盖
#   先到事件，造成 `node_miss=1` 类静默数据丢失。
#
#   永久修复：在 IndexInstaller 中创建 `REQUIRE n._elementId IS UNIQUE`
#   约束，让撞值在写入瞬间 fail-loud。但约束创建本身在已有撞值的库上
#   会失败，必须先用本脚本清理存量。
#
# 用法:
#     ./scripts/deploy/elementid-dedup.sh                       # dry-run
#     ./scripts/deploy/elementid-dedup.sh --apply               # 真删
#     PRIMARY=neo4j-primary ./scripts/deploy/elementid-dedup.sh # 自定容器名
#
# 删除策略:
#   每个 `_elementId` 撞值组里，**保留 _created_at 最早的那一条**
#   （对应 reconciler/sync-applier 写入的"上游真身"），DETACH DELETE
#   其余的（这些通常是事后客户端创建的、内部 id 撞了上游 prop 值的
#   "副本"）。删除后建议立刻：
#     1) 重启 ha-agent 让 IndexInstaller 创建 UNIQUE 约束
#     2) 触发一次 fullsync (POST /cluster/fullsync) 让 standby 与 primary
#        重新对齐 — standby 上对应槽位本来就被覆盖成了"副本"那一份，
#        fullsync 会用 primary 上保留的"上游真身"覆盖回来。

set -uo pipefail

PRIMARY="${PRIMARY:-neo4j-primary}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-}"
DATABASE="${DATABASE:-neo4j}"
APPLY=0

for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=1 ;;
    -h|--help)
      sed -n '1,40p' "$0"
      exit 0 ;;
    *)
      echo "Unknown arg: $arg (use --apply to actually delete)" >&2
      exit 2 ;;
  esac
done

if [[ -z "$NEO4J_PASSWORD" ]]; then
  echo "ERROR: set NEO4J_PASSWORD (export NEO4J_PASSWORD=...)" >&2
  exit 2
fi

cypher() {
  docker exec -i "$PRIMARY" cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" \
    -d "$DATABASE" --format plain "$1"
}

echo "=== BUG-083 _elementId dedup on container '$PRIMARY' database '$DATABASE' ==="
echo "Mode: $([[ $APPLY -eq 1 ]] && echo APPLY || echo DRY-RUN)"
echo

echo "--- summary: duplicate `_elementId` groups ---"
cypher "
  MATCH (n) WHERE n._elementId IS NOT NULL
  WITH n._elementId AS eid, count(*) AS c
  WHERE c > 1
  RETURN count(eid) AS dup_eid_groups, sum(c) AS poisoned_node_total
"

echo
echo "--- preview: first 50 dup groups (keep the earliest _created_at, delete the rest) ---"
cypher "
  MATCH (n) WHERE n._elementId IS NOT NULL
  WITH n._elementId AS eid, collect(n) AS ns
  WHERE size(ns) > 1
  WITH eid, ns,
       reduce(keep = head(ns), m IN tail(ns) |
              CASE WHEN coalesce(m._created_at, 9223372036854775807) <
                        coalesce(keep._created_at, 9223372036854775807)
                   THEN m ELSE keep END) AS keep_node
  WITH eid, keep_node, [m IN ns WHERE elementId(m) <> elementId(keep_node)] AS to_delete
  RETURN eid AS prop_eid,
         elementId(keep_node) AS keep_real_eid,
         coalesce(keep_node._created_at, 0) AS keep_cat,
         [m IN to_delete | {real_eid: elementId(m), seq: m.seq, id: m.id, cat: m._created_at}] AS to_delete
  ORDER BY prop_eid
  LIMIT 50
"

if [[ $APPLY -eq 0 ]]; then
  echo
  echo "Dry-run only. Re-run with --apply to delete the duplicate ghosts."
  exit 0
fi

echo
echo "--- applying DETACH DELETE on losers (this commits) ---"
cypher "
  MATCH (n) WHERE n._elementId IS NOT NULL
  WITH n._elementId AS eid, collect(n) AS ns
  WHERE size(ns) > 1
  WITH eid, ns,
       reduce(keep = head(ns), m IN tail(ns) |
              CASE WHEN coalesce(m._created_at, 9223372036854775807) <
                        coalesce(keep._created_at, 9223372036854775807)
                   THEN m ELSE keep END) AS keep_node
  UNWIND ns AS candidate
  WITH candidate, keep_node WHERE elementId(candidate) <> elementId(keep_node)
  DETACH DELETE candidate
  RETURN count(*) AS deleted
"

echo
echo "--- verification: residual dup groups (expect 0) ---"
cypher "
  MATCH (n) WHERE n._elementId IS NOT NULL
  WITH n._elementId AS eid, count(*) AS c
  WHERE c > 1
  RETURN count(eid) AS residual_dup_eid_groups
"

echo
echo "Done. Next steps:"
echo "  1. Restart ha-agent so IndexInstaller installs the UNIQUE constraint."
echo "  2. POST /cluster/fullsync on each standby to realign data with the cleaned primary."
