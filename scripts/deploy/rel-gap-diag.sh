#!/usr/bin/env bash
# rel-gap-diag.sh — 诊断 ha-load-switchover-test.py 报出的 rel_miss / rel_extra
#
# 用法（在项目根下）:
#     ./scripts/deploy/rel-gap-diag.sh
#     ./scripts/deploy/rel-gap-diag.sh path/to/report.json
#     ./scripts/deploy/rel-gap-diag.sh report.json w-bbfe63   # 自定义 writer_id
#
# 产出:
#     /tmp/rel-gap-diag-<UTC>.log   所有原始输出
#
# 依赖: docker, jq; redis-cli 或 docker redis:7-alpine（--network host）

set -uo pipefail

# ---------------------------------------------------------------- args
REPORT="${1:-load-switchover-report.json}"
WRITER_ID_OVERRIDE="${2:-}"

if [[ ! -f "$REPORT" ]]; then
  echo "ERROR: report JSON not found: $REPORT" >&2
  exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq not installed (apt install jq)" >&2
  exit 2
fi

# ---------------------------------------------------------------- env
ENV_FILE="${ENV_FILE:-docker/.env}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
fi
: "${REDIS_HOST:=127.0.0.1}"
: "${REDIS_PORT:=6379}"
: "${STREAM_KEY:=neo4j:cdc:neo4j:changes}"

if [[ -z "${NEO4J_PASSWORD:-}" ]]; then
  echo "ERROR: NEO4J_PASSWORD not set (expected in $ENV_FILE)" >&2
  exit 2
fi

# ---------------------------------------------------------------- writer_id & samples
WRITER_ID="${WRITER_ID_OVERRIDE:-$(jq -r '[.per_node[] | .writer_id? // empty] | first // (.writer_id? // empty)' "$REPORT")}"
if [[ -z "$WRITER_ID" || "$WRITER_ID" == "null" ]]; then
  echo "WARN: 没能从 report 里解出 writer_id；请作为第 2 参数显式提供（例 w-bbfe63）" >&2
  exit 2
fi

MISS_PAIRS_JSON=$(jq -c '[.per_node[] | .rel_missing_sample[]? ] | unique' "$REPORT")
EXTRA_PAIRS_JSON=$(jq -c '[.per_node[] | .rel_extra_sample[]? ] | unique' "$REPORT")

TS=$(date -u +%Y%m%dT%H%M%SZ)
LOG="/tmp/rel-gap-diag-${TS}.log"
: >"$LOG"

say() { printf '%s\n' "$*" | tee -a "$LOG"; }
hdr() { printf '\n========== %s ==========\n' "$*" | tee -a "$LOG"; }

hdr "config"
say "report      : $REPORT"
say "writer_id   : $WRITER_ID"
say "redis       : ${REDIS_HOST}:${REDIS_PORT}"
say "stream      : ${STREAM_KEY}"
say "miss_pairs  : $(jq 'length' <<<"$MISS_PAIRS_JSON") unique"
say "extra_pairs : $(jq 'length' <<<"$EXTRA_PAIRS_JSON") unique"
say "full log    : $LOG"

# ---------------------------------------------------------------- helpers
cypher_on() {
  local svc="$1"
  shift
  docker exec -i "$svc" cypher-shell \
    -u neo4j -p "$NEO4J_PASSWORD" --format plain \
    "$@" 2>&1
}

if command -v redis-cli >/dev/null 2>&1; then
  RCLI=(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT")
  [[ -n "${REDIS_PASSWORD:-}" ]] && RCLI+=(-a "$REDIS_PASSWORD" --no-auth-warning)
  redis_cli() { "${RCLI[@]}" "$@"; }
else
  redis_cli() {
    local args=(-h "$REDIS_HOST" -p "$REDIS_PORT")
    [[ -n "${REDIS_PASSWORD:-}" ]] && args+=(-a "$REDIS_PASSWORD" --no-auth-warning)
    docker run --rm --network host redis:7-alpine redis-cli "${args[@]}" "$@"
  }
fi

NEO4J_SVCS=(neo4j-primary neo4j-standby-1 neo4j-standby-2)
for s in "${NEO4J_SVCS[@]}"; do
  if ! docker ps --format '{{.Names}}' | grep -qx "$s"; then
    say "WARN: 容器 $s 未在 docker ps 中；脚本假设 compose 组是 neo4j-primary/standby-1/standby-2"
  fi
done

# ---------------------------------------------------------------- STEP 1
hdr "STEP 1 — 三节点 rel 基线（writer=$WRITER_ID）"
for svc in "${NEO4J_SVCS[@]}"; do
  # shellcheck disable=SC2016
  cnt=$(
    cypher_on "$svc" <<SQL | awk 'NR==2 { print $1 }'
MATCH (a:TestNode {writer: '$WRITER_ID'})-[r:RELATED_TO]->(b:TestNode {writer: '$WRITER_ID'})
RETURN count(r) AS c;
SQL
  )
  say "  $svc  rel_count(writer=$WRITER_ID) = $cnt"
done

# ---------------------------------------------------------------- STEP 2
hdr "STEP 2 — 是否存在跨 writer / null-writer 的 rel（问题 B 的总分）"
for svc in "${NEO4J_SVCS[@]}"; do
  say ""
  say "--- $svc ---"
  cypher_on "$svc" <<SQL | tee -a "$LOG"
MATCH (a:TestNode)-[r:RELATED_TO]->(b:TestNode)
WITH a, b, r,
     CASE
       WHEN a.writer = b.writer AND a.writer = '$WRITER_ID' THEN 'this-run'
       WHEN a.writer = b.writer AND a.writer IS NOT NULL    THEN 'other-run'
       WHEN a.writer IS NULL OR b.writer IS NULL            THEN 'null-writer'
       ELSE 'cross-writer'
     END AS kind
RETURN kind, count(r) AS cnt ORDER BY cnt DESC;
SQL
done
say ""
say "解读:"
say "  this-run == 873/858 左右 正常"
say "  other-run/null-writer/cross-writer > 0 → --clean-before-run 没清干净，rel_extra 基本都是残留"

# ---------------------------------------------------------------- STEP 3
hdr "STEP 3 — rel_extra 样本端点构成（前 10 对）"
echo "$EXTRA_PAIRS_JSON" | jq -c '.[]' | head -10 | while read -r pair; do
  a=$(jq '.[0]' <<<"$pair")
  b=$(jq '.[1]' <<<"$pair")
  say ""
  say "--- pair (seq_from=$a, seq_to=$b) on neo4j-primary ---"
  cypher_on neo4j-primary <<SQL | tee -a "$LOG"
MATCH (a:TestNode {seq: $a})-[r:RELATED_TO]->(b:TestNode {seq: $b})
RETURN a.id         AS a_id,
       a.writer     AS a_writer,
       a._elementId AS a_eid,
       b.id         AS b_id,
       b.writer     AS b_writer,
       b._elementId AS b_eid,
       r._elementId AS rel_eid,
       r.createdAt  AS rel_created_at;
SQL
done
say ""
say "解读:"
say "  a_writer != b_writer 或任一为 null  ⇒  残留污染 (问题 B 根因是 clean 漏网)"
say "  两端都是 $WRITER_ID 但 seq 组合诡异 ⇒  真实的 endpoint 错绑 bug (BUG-054 变体)"

# ---------------------------------------------------------------- STEP 4
hdr "STEP 4 — rel_miss 产/消端定位 (producer-side vs consumer-side)"
STREAM_DUMP="/tmp/rel-gap-diag-${TS}.stream.txt"
say "dumping stream '$STREAM_KEY' → $STREAM_DUMP (可能需要几秒)"
if redis_cli XRANGE "$STREAM_KEY" - + >"$STREAM_DUMP" 2>>"$LOG"; then
  say "stream entries: $(wc -l <"$STREAM_DUMP")"
else
  say "WARN: XRANGE 失败，producer/consumer 判定跳过"
  STREAM_DUMP=""
fi

echo "$MISS_PAIRS_JSON" | jq -c '.[]' | while read -r pair; do
  a=$(jq '.[0]' <<<"$pair")
  b=$(jq '.[1]' <<<"$pair")
  say ""
  say "--- miss pair (seq_from=$a, seq_to=$b) ---"
  row=$(
    cypher_on neo4j-primary <<SQL
MATCH (a:TestNode {seq: $a, writer: '$WRITER_ID'})-[r:RELATED_TO]->(b:TestNode {seq: $b, writer: '$WRITER_ID'})
RETURN r._elementId, a._elementId, b._elementId, r.createdAt;
SQL
  )
  data=$(echo "$row" | awk 'NR==2')
  if [[ -z "$data" || "$data" == *"0 rows"* ]]; then
    say "  NOT ON PRIMARY ⇒ 这对 rel 在 primary 也不存在（记账 artifact）"
    continue
  fi
  rel_eid=$(echo "$data" | awk -F', ' '{ gsub(/"/,"",$1); print $1 }')
  a_eid=$(echo "$data" | awk -F', ' '{ gsub(/"/,"",$2); print $2 }')
  b_eid=$(echo "$data" | awk -F', ' '{ gsub(/"/,"",$3); print $3 }')
  ct=$(echo "$data" | awk -F', ' '{ print $4 }')
  say "  rel_eid   = $rel_eid"
  say "  a_eid     = $a_eid"
  say "  b_eid     = $b_eid"
  say "  createdAt = $ct"

  if [[ -n "$STREAM_DUMP" && -n "$rel_eid" ]]; then
    hits=$(grep -Fc "$rel_eid" "$STREAM_DUMP" 2>/dev/null || echo 0)
    say "  rel_eid 在 stream 出现次数 = $hits"
    if ((hits == 0)); then
      say "  ⇒ PRODUCER-SIDE 丢（CDC 没把这条 rel CREATE 事件发上 Redis）"
    else
      say "  ⇒ CONSUMER-SIDE 丢（事件在 stream 里但 standby 没落地）"
      grep -F "$rel_eid" "$STREAM_DUMP" | head -3 | tee -a "$LOG"
    fi
  fi
done

# ---------------------------------------------------------------- STEP 5
hdr "STEP 5 — sync-applier consumer group 状态"
redis_cli XINFO GROUPS "$STREAM_KEY" 2>&1 | tee -a "$LOG" || true
say ""
say "每个 consumer group 前 20 条 pending（>0 说明 XACK 卡住）:"
groups=$(redis_cli XINFO GROUPS "$STREAM_KEY" 2>/dev/null | awk '/^name$/ { getline; print }')
for g in $groups; do
  say ""
  say "--- group: $g ---"
  redis_cli XPENDING "$STREAM_KEY" "$g" 2>&1 | tee -a "$LOG" || true
  redis_cli XPENDING "$STREAM_KEY" "$g" - + 20 2>&1 | tee -a "$LOG" || true
done

# ---------------------------------------------------------------- STEP 6
hdr "STEP 6 — ha-agent 近期 sync-applier / rel 相关告警"
if docker ps --format '{{.Names}}' | grep -qx ha-agent; then
  docker logs --tail 3000 ha-agent 2>&1 |
    grep -iE 'rel|sync-applier|applier|REL_MERGE|CypherTemplates|WITH is required|0 rows' |
    tail -60 | tee -a "$LOG" || true
else
  say "(ha-agent 容器不存在，跳过)"
fi

# ---------------------------------------------------------------- verdict
hdr "FINAL VERDICT"
say ""
say "→ 看 STEP 2:"
say "   other-run / null-writer / cross-writer 计数 > 0 ⇒ rel_extra 那 23 条是残留污染，不是真 bug"
say "   全部为 0 ⇒ rel_extra 是 endpoint 错绑真 bug，走 BUG-054 方向"
say ""
say "→ 看 STEP 4:"
say "   'PRODUCER-SIDE 丢' 占多数 ⇒ CDC collector 漏发 → BUG-058/BUG-061 系列"
say "   'CONSUMER-SIDE 丢' 占多数 ⇒ sync-applier 漏落 → BUG-079 stub 兜底被绕过"
say "   'NOT ON PRIMARY' 占多数  ⇒ phantom write 记账问题，不是一致性 bug"
say ""
say "完整原始输出: $LOG"
