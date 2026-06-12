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
     * @return true if the afterAsync queue drained AND all triggers were
     *         dropped successfully; false if either step failed (caller
     *         should mark the node for deferred cleanup and retry later).
     */
    /**
     * Convenience overload for callers that only ever manage the default
     * {@code "neo4j"} user database (e.g. {@code OldPrimaryRecovery}).
     */
    public static boolean uninstall(Driver driver) {
        return uninstall(driver, "neo4j");
    }

    public static boolean uninstall(Driver driver, String database) {
        // BUG-061: drain afterAsync queue FIRST — anything still pending at
        // drop-time will be silently discarded by APOC.
        boolean drained = ApocTriggerInstaller.drainRelTriggerAfterAsync(driver, database);
        if (!drained) {
            log.warn("APOC afterAsync queue did NOT confirm drained on '{}' before trigger drop; "
                + "any pending rel stamps may be lost. Proceeding with drop anyway.", database);
        }
        boolean allOk = drained;
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
        return allOk;
    }
}
