package com.neo4j.ha.agent.recovery;

import com.neo4j.ha.agent.bootstrap.ApocTriggerInstaller;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ApocTriggerUninstaller {

    private static final Logger log = LoggerFactory.getLogger(ApocTriggerUninstaller.class);

    // BUG-063: node timestamp trigger was split into three independent
    // triggers (created / assigned / removed). Also keep the legacy
    // 'cdc-timestamp' name in the drop list so an upgrade from a pre-fix
    // installation cleans up the stale body left in system DB.
    private static final List<String> TRIGGER_NAMES = List.of(
        "cdc-timestamp",              // legacy (pre-BUG-063), best-effort cleanup
        "cdc-timestamp-created",      // BUG-063
        "cdc-timestamp-assigned",     // BUG-063
        "cdc-timestamp-removed",      // BUG-063
        "cdc-rel-timestamp",
        "cdc-capture-node-deletes",
        "cdc-capture-rel-deletes"
    );

    /**
     * Drop the CDC APOC triggers from the given node.
     *
     * BUG-046 fix: APOC 5.x requires {@code apoc.trigger.drop} to execute against the
     * {@code system} database (same as {@code apoc.trigger.install}). Executing against
     * the default user database silently fails and the triggers stay alive on the node.
     *
     * When an "old primary" is demoted to standby we MUST uninstall its triggers so that
     * subsequent {@code SyncApplier} writes on this node do NOT set {@code _updated_at =
     * timestamp()} via the trigger. Leaving triggers in place causes synced data to be
     * "bumped" to the new primary's local wall clock, breaking the CDC keyset cursor
     * contract when this node later becomes primary again (see BUG-045 interaction).
     *
     * <p>BUG-061: this is now an <b>atomic "drain + drop"</b>. Before we
     * call {@code apoc.trigger.drop('cdc-rel-timestamp', ...)} we first
     * flush the APOC {@code afterAsync} queue on the target DB via
     * {@link ApocTriggerInstaller#drainRelTriggerAfterAsync(Driver, String)}.
     * Without that drain, any rel-stamp tasks still sitting in the queue
     * at drop-time are discarded — producing "naked" relationships
     * ({@code _elementId IS NULL AND _updated_at IS NULL}) on the node
     * that was primary at commit time. Those rels are invisible to
     * keyset-paginated CDC polling and permanently diverge the cluster.
     *
     * <p>Callers should ensure writes are already blocked and in-flight
     * business transactions have drained (e.g. via
     * {@code InflightTxDrainWaiter}) before invoking {@code uninstall}.
     * With writes blocked, the drain probe reaches steady state in a
     * single round-trip on the afterAsync executor's next tick.
     *
     * @param driver   Bolt driver to the (old) primary whose triggers
     *                 are being removed. The drain probe runs against
     *                 this driver; the drop itself runs against
     *                 {@code system} on the same driver.
     * @param database the user database whose triggers should be removed
     *                 (also the DB the drain probe writes its sentinel to).
     * @return see {@link UninstallResult}. REVIEW-R1: the drain result is
     *         reported separately from the drop result — they used to be
     *         AND-ed into one boolean, which made a harmless "nothing to
     *         drain" look like an uninstall failure.
     */
    /** Name of the afterAsync rel-stamp trigger whose queue must be drained first. */
    private static final String REL_TIMESTAMP_TRIGGER_NAME = "cdc-rel-timestamp";

    /**
     * Outcome of an uninstall attempt. REVIEW-R1 split this out of the old
     * single {@code boolean} return.
     *
     * @param triggersDropped all trigger definitions are gone from the system DB.
     *                        This is the ONLY thing a caller should gate a
     *                        "is it safe to treat this node as a standby"
     *                        decision on.
     * @param afterAsyncDrained the APOC afterAsync queue was confirmed flushed
     *                        before the drop. Informational: a false here means
     *                        some rel stamps may have been discarded (naked rels
     *                        that {@code NakedRelationshipHealer} will repair),
     *                        NOT that the triggers are still armed.
     * @param drainSkipped   the rel-stamp trigger was not installed, so there was
     *                        no queue to drain and no probe was run.
     */
    public record UninstallResult(boolean triggersDropped,
                                  boolean afterAsyncDrained,
                                  boolean drainSkipped) {}

    /**
     * Convenience overload for callers that only ever manage the default
     * {@code "neo4j"} user database.
     *
     * @deprecated REVIEW-H3 — the user database is configurable per node
     *     ({@code cluster.nodes[].neo4j.database}); hardcoding "neo4j" silently
     *     targets the wrong database on any non-default deployment. Pass the
     *     configured database explicitly.
     */
    @Deprecated
    public static boolean uninstall(Driver driver) {
        return uninstall(driver, "neo4j");
    }

    public static boolean uninstall(Driver driver, String database) {
        return uninstallDetailed(driver, database).triggersDropped();
    }

    /**
     * REVIEW-R1: the old code returned {@code allOk = drained && droppedOk}, and
     * {@code OldPrimaryRecovery} used that single boolean to decide whether to
     * ABORT the whole recovery. That conflation created a deterministic dead end:
     *
     * <ol>
     *   <li>failover Phase 3.5 unconditionally marks the old primary
     *       pendingCleanup, so recovery always runs for it;</li>
     *   <li>if Phase 9 already cleaned that node up successfully (the old primary
     *       was still reachable — e.g. a network blip caused the failover), its
     *       triggers are ALREADY gone;</li>
     *   <li>recovery then calls uninstall again, whose drain probe creates a
     *       sentinel relationship and waits for {@code _updated_at} to appear —
     *       but with no trigger installed nothing will ever stamp it, so the
     *       probe burns the full 30 s timeout and returns false;</li>
     *   <li>{@code allOk} is false ⇒ recovery aborts ⇒ the node never rejoins,
     *       and every DOWN→UP transition repeats the 30 s stall on the single
     *       ha-failover thread (blocking real failovers queued behind it).</li>
     * </ol>
     *
     * Fixed two ways: the drain result no longer contaminates the drop result,
     * and the drain is skipped entirely when the trigger isn't installed.
     */
    public static UninstallResult uninstallDetailed(Driver driver, String database) {
        // BUG-061: drain afterAsync queue FIRST — anything still pending at
        // drop-time will be silently discarded by APOC. REVIEW-R1: but only if
        // the trigger that feeds that queue actually exists.
        boolean relTriggerPresent = ApocTriggerInstaller.isTriggerInstalled(
            driver, database, REL_TIMESTAMP_TRIGGER_NAME);
        boolean drained = false;
        boolean drainSkipped = !relTriggerPresent;
        if (relTriggerPresent) {
            drained = ApocTriggerInstaller.drainRelTriggerAfterAsync(driver, database);
            if (!drained) {
                log.warn("APOC afterAsync queue did NOT confirm drained on '{}' before trigger drop; "
                    + "any pending rel stamps may be lost (NakedRelationshipHealer will repair them). "
                    + "Proceeding with drop — this does NOT block the uninstall.", database);
            }
        } else {
            log.info("APOC trigger '{}' is not installed on '{}'; skipping afterAsync drain probe "
                + "(REVIEW-R1: probing here would block for the full timeout and then be "
                + "misread as an uninstall failure)", REL_TIMESTAMP_TRIGGER_NAME, database);
        }

        boolean allOk = true;
        try (Session session = driver.session(SessionConfig.forDatabase("system"))) {
            for (String name : TRIGGER_NAMES) {
                try {
                    session.run("CALL apoc.trigger.drop($db, $name)",
                        Map.of("db", database, "name", name)).consume();
                    log.info("APOC trigger '{}' uninstalled", name);
                } catch (Exception e) {
                    String msg = e.getMessage() == null ? "" : e.getMessage();
                    // Two benign cases: trigger was already dropped, or never installed
                    // (e.g. fresh node after re-bootstrap). Anything else is a real failure.
                    boolean benign = msg.contains("not found")
                        || msg.contains("NotFound")
                        || msg.contains("does not exist");
                    if (benign) {
                        log.info("APOC trigger '{}' already absent", name);
                    } else {
                        log.warn("Failed to uninstall APOC trigger '{}': {}", name, msg);
                        allOk = false;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to open 'system' database session for trigger uninstall: {}",
                e.getMessage(), e);
            allOk = false;
        }
        return new UninstallResult(allOk, drained, drainSkipped);
    }
}
