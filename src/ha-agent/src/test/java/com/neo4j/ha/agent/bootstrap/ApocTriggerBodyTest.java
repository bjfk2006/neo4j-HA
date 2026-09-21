package com.neo4j.ha.agent.bootstrap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BUG-089 回归：`cdc-capture-rel-deletes`（phase:'before'）**绝不能读端点节点的属性**。
 *
 * <p>历史：BUG-082 为了 scoped delete 引入
 * {@code coalesce(startNode(dr)._elementId, elementId(startNode(dr)))}，在
 * {@code DETACH DELETE n} 下端点节点已在同一 tx 内被标删，属性读抛
 * {@code EntityNotFoundException: Node with id N has been deleted in this transaction}。
 * 按 phase:'before' 的语义，任一 trigger 抛异常会 terminate 整个事务——**业务删除
 * 本身失败**，而不只是丢一条 CDC 事件。香港测试环境 2026-07-20 → 09-19 累计 5745 次。
 *
 * <p>这一类 bug 在本仓库已经复发过四次（BUG-064 / 065 / 066 / 089），每次都是
 * "在 before 触发器里对将删实体多调了一次函数"。所以把契约钉死在单测里：
 * 删除触发器的 body 里不允许出现 startNode/endNode，端点身份只能来自创建时
 * stamp 在关系自身上的属性。
 */
class ApocTriggerBodyTest {

    @Test
    void relDeleteTrigger_neverTouchesEndpointNodes() {
        String body = ApocTriggerInstaller.REL_DELETE_TRIGGER;

        assertFalse(body.contains("startNode("),
                "before 触发器不得解析 startNode——端点可能已在同 tx 被删除");
        assertFalse(body.contains("endNode("),
                "before 触发器不得解析 endNode——端点可能已在同 tx 被删除");
        assertFalse(body.contains("sn._elementId") || body.contains("en._elementId"),
                "不得读端点节点属性（getProperty 会撞 in-tx-deleted 检查）");
    }

    @Test
    void relDeleteTrigger_readsEndpointIdsFromRemovedProperties() {
        String body = ApocTriggerInstaller.REL_DELETE_TRIGGER;

        assertTrue(body.contains("$removedRelationshipProperties[\"_startElementId\"]"),
                "端点身份必须从被删关系自身的旧属性里取");
        assertTrue(body.contains("$removedRelationshipProperties[\"_endElementId\"]"),
                "端点身份必须从被删关系自身的旧属性里取");
        // 取不到时该属性缺省（null），applier 回落 BUG-082 保留的 legacy REL_DELETE。
        // 绝不能写空串：applier 的分支判据看的是"有没有值"，空串会走 scoped 分支、
        // 匹配不到任何关系，删除静默丢失。
        assertFalse(body.contains("coalesce(drStartEid, \"\")"),
                "端点 id 缺失时必须留 null，不能降级成空串");
        assertFalse(body.contains("coalesce(drEndEid, \"\")"),
                "端点 id 缺失时必须留 null，不能降级成空串");
    }

    @Test
    void relTimestampTrigger_stampsEndpointIdsAtCreateTime() {
        String body = ApocTriggerInstaller.REL_TIMESTAMP_TRIGGER;

        assertTrue(body.contains("_startElementId"), "创建时必须 stamp 起点 id");
        assertTrue(body.contains("_endElementId"), "创建时必须 stamp 终点 id");
        // 这个触发器是 afterAsync：关系刚创建、端点都活着，读属性安全
        assertTrue(body.contains("phase: 'afterAsync'"),
                "stamp 必须留在 afterAsync 阶段；挪到 before 会把同样的坑搬回来");
    }
}
