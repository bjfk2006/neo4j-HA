package com.neo4j.ha.sync.applier;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CypherTemplatesTest {

    @Test
    void nodeMerge_containsMergeKeyword() {
        assertTrue(CypherTemplates.NODE_MERGE.contains("MERGE"),
                "NODE_MERGE should contain MERGE clause");
    }

    @Test
    void nodeMerge_containsSetKeyword() {
        assertTrue(CypherTemplates.NODE_MERGE.contains("SET"),
                "NODE_MERGE should contain SET clause");
    }

    @Test
    void nodeMerge_containsElementIdBinding() {
        assertTrue(CypherTemplates.NODE_MERGE.contains("$elementId"),
                "NODE_MERGE should bind $elementId parameter");
    }

    @Test
    void nodeMerge_containsPropertiesBinding() {
        assertTrue(CypherTemplates.NODE_MERGE.contains("$properties"),
                "NODE_MERGE should bind $properties parameter");
    }

    @Test
    void nodeMerge_containsFormatPlaceholder() {
        assertTrue(CypherTemplates.NODE_MERGE.contains("%s"),
                "NODE_MERGE should contain %s placeholder for label");
    }

    @Test
    void nodeDelete_containsMatchKeyword() {
        assertTrue(CypherTemplates.NODE_DELETE.contains("MATCH"),
                "NODE_DELETE should contain MATCH clause");
    }

    @Test
    void nodeDelete_containsDeleteKeyword() {
        assertTrue(CypherTemplates.NODE_DELETE.contains("DELETE"),
                "NODE_DELETE should contain DELETE clause");
    }

    @Test
    void nodeDelete_containsDetachDelete() {
        assertTrue(CypherTemplates.NODE_DELETE.contains("DETACH DELETE"),
                "NODE_DELETE should use DETACH DELETE to remove relationships");
    }

    @Test
    void nodeDelete_containsFormatPlaceholder() {
        assertTrue(CypherTemplates.NODE_DELETE.contains("%s"),
                "NODE_DELETE should contain %s placeholder for label");
    }

    @Test
    void relMerge_containsStartLabelPlaceholder() {
        assertTrue(CypherTemplates.REL_MERGE.contains("%1$s"),
                "REL_MERGE should contain %1$s for start node labels");
    }

    @Test
    void relMerge_containsEndLabelPlaceholder() {
        assertTrue(CypherTemplates.REL_MERGE.contains("%2$s"),
                "REL_MERGE should contain %2$s for end node labels");
    }

    @Test
    void relMerge_containsRelTypePlaceholder() {
        assertTrue(CypherTemplates.REL_MERGE.contains("%3$s"),
                "REL_MERGE should contain %3$s for relationship type");
    }

    // BUG-078 regression tests (2026-04-17): REL_MERGE was rewritten from
    // `MERGE (a)-[r:T {_elementId:$eid}]->(b) SET r = $props`
    // to
    // `OPTIONAL MATCH ()-[stale:T {_elementId:$eid}]->() DELETE stale
    //  CREATE (a)-[r:T]->(b) SET r = $props`
    // because Neo4j 5.x MERGE + DELETE + MERGE of the same _elementId in one
    // transaction silently evaluates to zero relationships at commit (bug
    // reproduced empirically). The pattern-name is kept as `REL_MERGE` for
    // call-site stability. See CypherTemplates.REL_MERGE javadoc and
    // ha-agent-design.md BUG-078 entry for the full story.

    @Test
    void relMerge_bug078_noMergeForTheRelItself() {
        // MERGE on the rel pattern (a)-[r:T {_elementId:X}]->(b) triggers the
        // BUG-078 ghost-rel bug when CREATE→DELETE→CREATE with the same
        // _elementId lands in one tx. MERGE on endpoint NODES is fine (that's
        // BUG-079's fix) — but the rel MUST be created via explicit CREATE.
        String q = CypherTemplates.REL_MERGE;
        // Strip out "MERGE (a:..." and "MERGE (b:..." (the BUG-079 endpoint MERGEs)
        // and verify no other MERGE remains that could be a rel-level MERGE.
        String stripped = q
            .replaceAll("MERGE \\(a:[^)]+\\)", "")
            .replaceAll("MERGE \\(b:[^)]+\\)", "");
        assertFalse(stripped.contains("MERGE"),
                "REL_MERGE must not contain any MERGE other than the two endpoint-node MERGEs. "
                + "A MERGE on the rel pattern would reintroduce BUG-078.");
    }

    @Test
    void relMerge_bug078_usesOptionalMatchDeleteCreate() {
        String q = CypherTemplates.REL_MERGE;
        assertTrue(q.contains("OPTIONAL MATCH"),
                "REL_MERGE must OPTIONAL MATCH any pre-existing rel with the same _elementId");
        assertTrue(q.contains("DELETE stale"),
                "REL_MERGE must explicitly DELETE the stale ghost");
        assertTrue(q.contains("CREATE (a)-"),
                "REL_MERGE must CREATE a fresh relationship (not MERGE)");
    }

    @Test
    void relMerge_bug078_bindsBothElementIdAndProperties() {
        String q = CypherTemplates.REL_MERGE;
        // $relElementId must be used on both the sweep (stale) and the new rel
        long relElemBindCount = q.chars().filter(c -> c == '$').count();
        assertTrue(relElemBindCount >= 4,
                "REL_MERGE should bind $startNodeId, $endNodeId, $relElementId (twice), $properties — "
                + "at least 4 bind-markers; got template=\n" + q);
        assertTrue(q.contains("SET r._elementId = $relElementId"),
                "REL_MERGE must stamp _elementId on the newly created rel");
    }

    @Test
    void relMerge_canBeFormatted() {
        String formatted = CypherTemplates.REL_MERGE.formatted("Person", "Company", "WORKS_AT");
        assertTrue(formatted.contains("Person"), "Formatted REL_MERGE should contain start label");
        assertTrue(formatted.contains("Company"), "Formatted REL_MERGE should contain end label");
        assertTrue(formatted.contains("WORKS_AT"), "Formatted REL_MERGE should contain rel type");
        // Rel type appears in both the stale-match and the new CREATE — count usages.
        int worksAtOccurrences = (formatted.length() - formatted.replace("WORKS_AT", "").length())
                                  / "WORKS_AT".length();
        assertEquals(2, worksAtOccurrences,
                "Rel type must appear exactly twice (OPTIONAL MATCH stale + CREATE new)");
    }

    // BUG-079 regression tests (2026-04-17): CdcCollector sorts emitted events
    // by `_updated_at` ASC, but a rel's `_updated_at` (afterAsync stamped right
    // at commit time of the rel-creating tx) can be LOWER than its endpoint
    // node's `_updated_at` when the node is subsequently mutated by the
    // workload. The result is a stream order of RELATIONSHIP_CREATED before
    // NODE_UPDATED for its endpoints. If REL_MERGE uses `MATCH` on endpoints,
    // the query returns zero rows (endpoints not yet on standby), and the
    // CREATE clause silently skips — event is XACK'd, rel lost. BUG-079 fix
    // changes MATCH → MERGE on the endpoint nodes so a stub gets created if
    // needed; later NODE_UPDATED event fills in properties on the same stub.

    @Test
    void relMerge_requiresWithBetweenMergeAndOptionalMatch() {
        String q = CypherTemplates.REL_MERGE;
        assertTrue(q.contains("WITH a, b"),
                "Neo4j requires WITH between endpoint MERGEs and OPTIONAL MATCH (syntax error otherwise)");
        int withIdx = q.indexOf("WITH a, b");
        int optIdx = q.indexOf("OPTIONAL MATCH");
        assertTrue(withIdx > 0 && optIdx > withIdx,
                "WITH a, b must appear before OPTIONAL MATCH");
    }

    @Test
    void relMerge_bug079_mergesEndpointsInsteadOfMatching() {
        String q = CypherTemplates.REL_MERGE;
        // We expect TWO MERGE statements for the endpoints (a and b). A
        // `MATCH (a:...)` would be a BUG-079 regression. Allow `MATCH` inside
        // the OPTIONAL MATCH of the stale rel, but not for the endpoints.
        String withoutOptionalMatch = q.replace("OPTIONAL MATCH", "");
        assertFalse(withoutOptionalMatch.contains("MATCH (a"),
                "REL_MERGE must not use `MATCH (a:...)` for the start endpoint — a rel event may "
                + "arrive before its endpoint's NODE_UPDATED, causing silent zero-row evaluation. "
                + "Use MERGE to create an eid-only stub instead (BUG-079).");
        assertFalse(withoutOptionalMatch.contains("MATCH (b"),
                "REL_MERGE must not use `MATCH (b:...)` for the end endpoint — same reason as start.");
        assertTrue(q.contains("MERGE (a:%1$s"),
                "Start endpoint MUST be MERGE'd with the label placeholder");
        assertTrue(q.contains("MERGE (b:%2$s"),
                "End endpoint MUST be MERGE'd with the label placeholder");
    }

    @Test
    void relMerge_bug079_endpointLabelPlaceholdersStillWork() {
        String formatted = CypherTemplates.REL_MERGE.formatted("Person", "Company", "WORKS_AT");
        // After BUG-079 the endpoint MERGEs inherit the labels. Make sure
        // formatting still substitutes them correctly (and exactly once each).
        int personCount = (formatted.length() - formatted.replace("Person", "").length()) / "Person".length();
        int companyCount = (formatted.length() - formatted.replace("Company", "").length()) / "Company".length();
        assertEquals(1, personCount, "start label should appear exactly once (in its MERGE clause)");
        assertEquals(1, companyCount, "end label should appear exactly once (in its MERGE clause)");
    }

    @Test
    void relDelete_containsFormatPlaceholder() {
        assertTrue(CypherTemplates.REL_DELETE.contains("%s"),
                "REL_DELETE should contain %s placeholder for relationship type");
    }

    @Test
    void relDelete_containsDeleteKeyword() {
        assertTrue(CypherTemplates.REL_DELETE.contains("DELETE"),
                "REL_DELETE should contain DELETE clause");
    }

    @Test
    void relDelete_containsRelElementIdBinding() {
        assertTrue(CypherTemplates.REL_DELETE.contains("$relElementId"),
                "REL_DELETE should bind $relElementId parameter");
    }

    @Test
    void nodeDeleteNoLabel_containsMatchWithoutLabel() {
        assertTrue(CypherTemplates.NODE_DELETE_NO_LABEL.contains("MATCH (n {"),
                "NODE_DELETE_NO_LABEL should match node without label constraint");
    }

    // BUG-082 regression tests (2026-04-22): Neo4j 5.x `_elementId` gets
    // recycled across transactions. Matching rels by `_elementId` alone
    // (in REL_MERGE's stale sweep or REL_DELETE) can silently delete a
    // completely unrelated live rel that inherits the recycled id.
    // Fix: scope rel-identity to (start_eid, end_eid, type, _elementId).

    @Test
    void relMerge_bug082_staleMatchIsEndpointScoped() {
        String q = CypherTemplates.REL_MERGE;
        assertTrue(q.contains("OPTIONAL MATCH (a)-[stale"),
                "REL_MERGE stale MATCH MUST be scoped to (a)-[stale]->(b); unbounded "
                + "`OPTIONAL MATCH ()-[stale {_elementId: X}]->()` deletes unrelated rels "
                + "under Neo4j _elementId recycling. See BUG-082.");
        assertFalse(q.contains("OPTIONAL MATCH ()-[stale"),
                "REL_MERGE must not use unbounded `OPTIONAL MATCH ()-[stale]->()`; "
                + "this reintroduces BUG-082 (unrelated rel deletion on _elementId reuse).");
    }

    @Test
    void relDeleteScoped_existsAndBindsEndpoints() {
        String q = CypherTemplates.REL_DELETE_SCOPED;
        assertTrue(q.contains("$startNodeId"),
                "REL_DELETE_SCOPED must bind $startNodeId");
        assertTrue(q.contains("$endNodeId"),
                "REL_DELETE_SCOPED must bind $endNodeId");
        assertTrue(q.contains("$relElementId"),
                "REL_DELETE_SCOPED must bind $relElementId");
        assertTrue(q.contains("%s"),
                "REL_DELETE_SCOPED must have %s placeholder for rel type");
        assertTrue(q.contains("DELETE"),
                "REL_DELETE_SCOPED must DELETE the matched rel");
    }

    @Test
    void relDeleteScoped_matchesSpecificEndpoints() {
        String formatted = CypherTemplates.REL_DELETE_SCOPED.formatted("RELATED_TO");
        // Pattern must anchor both endpoints (not unbounded ()-[...]->() pattern).
        assertTrue(formatted.contains("(a {_elementId: $startNodeId})")
                && formatted.contains("(b {_elementId: $endNodeId})"),
                "REL_DELETE_SCOPED must anchor pattern to both endpoint _elementIds; "
                + "unbounded pattern would still be vulnerable to BUG-082 id recycling.");
    }

    @Test
    void relDelete_legacyKeptForRollingUpgrade() {
        // The legacy REL_DELETE (elementId-only) is retained for backwards
        // compatibility with _CDCDeleteEvent transit nodes produced by a
        // pre-BUG-082 trigger version. RelationshipApplier.delete prefers
        // the scoped variant but falls back to the legacy form if the CDC
        // event lacks endpoint ids.
        String q = CypherTemplates.REL_DELETE;
        assertTrue(q.contains("$relElementId"),
                "Legacy REL_DELETE must still bind $relElementId");
        assertFalse(q.contains("$startNodeId"),
                "Legacy REL_DELETE must NOT require endpoint ids (it's the fallback)");
    }
}
