package com.neo4j.ha.agent.routing;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for {@link HaProxyStateSyncer#computeDesiredChanges}.
 *
 * Bug history:
 *   - BUG-026: first version tested only FMAINT bit (0x01) — missed `disabled` keyword
 *     servers whose admin_state is CMAINT=0x04.
 *   - BUG-032: second version hard-coded column 5 as srv_admin_state, but column 5 is
 *     srv_op_state; column 6 is srv_admin_state.
 *   - BUG-035: third version required primary admin_state == 0 to be considered ready.
 *     HAProxy's `show servers state` reports `backup disabled` servers as admin_state=4
 *     (CMAINT) even after an operator `set state ready` call, because the backup flag
 *     keeps the config-derived MAINT bit sticky in the state dump. The syncer looped
 *     every 10 s re-issuing `set state ready`, which HAProxy treated as no-op but that
 *     polluted logs and counters.
 *
 * Current version (post-BUG-035): use `srv_op_state == 2` (RUNNING) to judge whether the
 * primary is actually serving; use any admin-flag bit (mask 0x3F) to judge whether a
 * non-primary is out of the pool.
 */
class HaProxyStateSyncerTest {

    // Columns: be_id be_name srv_id srv_name srv_addr srv_op_state srv_admin_state srv_uweight
    //          srv_iweight srv_time_since_last_change srv_check_status srv_check_result
    //          srv_check_health srv_check_state srv_agent_state bk_f_forced_id srv_f_forced_id
    //          srv_fqdn srv_port srvrecord srv_use_ssl srv_check_port srv_check_addr
    //          srv_agent_addr srv_agent_port
    private static final String HEADER =
        "1\n" +
        "# be_id be_name srv_id srv_name srv_addr srv_op_state srv_admin_state srv_uweight srv_iweight srv_time_since_last_change srv_check_status srv_check_result srv_check_health srv_check_state srv_agent_state bk_f_forced_id srv_f_forced_id srv_fqdn srv_port srvrecord srv_use_ssl srv_check_port srv_check_addr srv_agent_addr srv_agent_port\n";

    @Test
    void steadyState_primaryRunningStandbysCMAINT_noCommands() {
        // Starting state right after HAProxy reads haproxy.cfg:
        //   primary:    op=RUNNING(2), admin=0
        //   standby-1:  op=STOPPED(0), admin=CMAINT(4)    (disabled in config)
        //   standby-2:  op=STOPPED(0), admin=CMAINT(4)    (disabled in config)
        String state = HEADER +
            "2 neo4j_primary 1 neo4j-primary   172.19.0.2 2 0 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n" +
            "2 neo4j_primary 2 neo4j-standby-1 172.19.0.3 0 4 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n" +
            "2 neo4j_primary 3 neo4j-standby-2 172.19.0.4 0 4 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n";

        var cmds = HaProxyStateSyncer.computeDesiredChanges(state, "neo4j-primary",
            List.of("neo4j-standby-1", "neo4j-standby-2"), true);

        assertTrue(cmds.isEmpty(), "boot-time state must be recognized as consistent; got " + cmds);
    }

    @Test
    void postSwitchover_backupPromotedToPrimary_noCommands() {
        // After switchover, the new primary is a former `backup disabled` server.
        // HaProxyUpdater sent `set state ready` to it. HAProxy logs "UP/READY (leaving
        // forced maintenance)" AND routes traffic to it — BUT the state dump reports
        // admin_state=4 (residual CMAINT) and op_state=2 (RUNNING). The syncer must not
        // loop issuing `set state ready` just because admin_state is non-zero.
        // (BUG-035 regression guard.)
        String state = HEADER +
            "2 neo4j_primary 1 neo4j-primary   172.19.0.2 0 1 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n" +
            "2 neo4j_primary 2 neo4j-standby-1 172.19.0.3 0 4 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n" +
            "2 neo4j_primary 3 neo4j-standby-2 172.19.0.4 2 4 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n";

        var cmds = HaProxyStateSyncer.computeDesiredChanges(state, "neo4j-standby-2",
            List.of("neo4j-primary", "neo4j-standby-1"), true);

        assertTrue(cmds.isEmpty(),
            "backup server that HAProxy considers RUNNING must be treated as serving; got " + cmds);
    }

    @Test
    void primaryStopped_emitsReadyCommand() {
        // Primary op_state=STOPPED (0). Should send `set state ready` to wake it up.
        String state = HEADER +
            "2 neo4j_primary 1 neo4j-primary   172.19.0.2 0 1 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n" +
            "2 neo4j_primary 2 neo4j-standby-1 172.19.0.3 2 4 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n";

        var cmds = HaProxyStateSyncer.computeDesiredChanges(state, "neo4j-primary",
            List.of("neo4j-standby-1"), true);

        assertEquals(1, cmds.size());
        assertTrue(cmds.get(0).contains("neo4j-primary"));
        assertTrue(cmds.get(0).endsWith("state ready"));
    }

    @Test
    void nonPrimaryServing_emitsMaintCommand() {
        // A non-primary with admin_state=0 is actively in the pool; must be pushed to maint.
        String state = HEADER +
            "2 neo4j_primary 1 neo4j-primary   172.19.0.2 2 0 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n" +
            "2 neo4j_primary 2 neo4j-standby-1 172.19.0.3 2 0 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n";

        var cmds = HaProxyStateSyncer.computeDesiredChanges(state, "neo4j-primary",
            List.of("neo4j-standby-1"), true);

        assertEquals(1, cmds.size());
        assertTrue(cmds.get(0).contains("neo4j-standby-1"));
        assertTrue(cmds.get(0).endsWith("state maint"));
    }

    @Test
    void primaryMissing_returnsNoCommands() {
        String state = HEADER +
            "2 neo4j_primary 2 neo4j-standby-1 172.19.0.3 2 4 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n";
        assertTrue(HaProxyStateSyncer.computeDesiredChanges(state, "neo4j-primary",
            List.of("neo4j-standby-1"), true).isEmpty());
    }

    @Test
    void nullState_returnsNoCommands() {
        assertTrue(HaProxyStateSyncer.computeDesiredChanges(null, "neo4j-primary", List.of(), true)
                .isEmpty());
    }

    @Test
    void regression_opStateNotConfusedWithAdminState() {
        // Column 5 is srv_op_state, column 6 is srv_admin_state. An early buggy parser
        // read column 5 as admin_state. Here the primary has op_state=2 (RUNNING) at col 5
        // and admin_state=0 at col 6 — the buggy parser would see "2" as admin_state and
        // incorrectly demand `set state ready`.
        String state = HEADER +
            "2 neo4j_primary 1 neo4j-primary   172.19.0.2 2 0 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n" +
            "2 neo4j_primary 2 neo4j-standby-1 172.19.0.3 0 4 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n";

        assertTrue(HaProxyStateSyncer.computeDesiredChanges(state, "neo4j-primary",
            List.of("neo4j-standby-1"), true).isEmpty());
    }

    @Test
    void bug069_primaryDownAndUnhealthy_doesNotEmitReady() {
        // BUG-069 regression: HAProxy's L4 probe detected primary DOWN (op_state=0) and
        // HealthChecker has flagged it non-HEALTHY (primaryHealthy=false). Before the fix,
        // the syncer would fire `set server neo4j-primary state ready`, fighting HAProxy's
        // verdict and causing client connection-refused bursts + log-level DOWN/ready
        // thrash. After the fix, the syncer defers to HAProxy and leaves the dead primary
        // marked DOWN; Failover (driven by BUG-068's L1/L2→DOWN escalation) will switch
        // clusterState.primaryNode onto a live node, which re-converges this sync pass.
        String state = HEADER +
            "2 neo4j_primary 1 neo4j-primary   172.19.0.2 0 1 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n" +
            "2 neo4j_primary 2 neo4j-standby-1 172.19.0.3 2 4 1 1 100 6 3 4 6 0 0 0 - 7687 - 0 0 - - 0\n";

        var cmds = HaProxyStateSyncer.computeDesiredChanges(state, "neo4j-primary",
            List.of("neo4j-standby-1"), /* primaryHealthy= */ false);

        assertTrue(cmds.isEmpty(),
            "when HealthChecker says primary is non-HEALTHY, must not revive it; got " + cmds);
    }
}
