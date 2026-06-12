package com.neo4j.ha.sync.fullsync;

import com.neo4j.ha.common.model.*;
import com.neo4j.ha.sync.consumer.FullSyncConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FullSyncReceiverTest {

    private DatabaseCleaner databaseCleaner;
    private FullSyncConsumer fullSyncConsumer;
    private FullSyncReceiver receiver;
    private Driver driver;

    @BeforeEach
    void setUp() {
        databaseCleaner = mock(DatabaseCleaner.class);
        fullSyncConsumer = mock(FullSyncConsumer.class);
        driver = mock(Driver.class);
        receiver = new FullSyncReceiver(databaseCleaner, fullSyncConsumer, "neo4j");
    }

    @Test
    void initialState_isIdle() {
        assertEquals(FullSyncReceiver.State.IDLE, receiver.getState());
    }

    @Test
    void isReceiving_initialState_returnsFalse() {
        assertFalse(receiver.isReceiving(),
                "Should not be receiving in IDLE state");
    }

    @Test
    void onFullSyncStart_transitionsToReceiving() {
        ChangeEvent event = makeFullSyncStartEvent(null);

        receiver.onFullSyncStart(event, driver, "node-1");

        assertEquals(FullSyncReceiver.State.RECEIVING, receiver.getState());
    }

    @Test
    void onFullSyncStart_cleansDatabase() {
        ChangeEvent event = makeFullSyncStartEvent(null);

        receiver.onFullSyncStart(event, driver, "node-1");

        verify(databaseCleaner).clean(driver, "neo4j");
    }

    @Test
    void onFullSyncStart_consumesFullSyncBatches() {
        ChangeEvent event = makeFullSyncStartEvent(null);

        receiver.onFullSyncStart(event, driver, "node-1");

        verify(fullSyncConsumer).consumeFullSyncBatches(driver, "neo4j", "node-1");
    }

    @Test
    void onFullSyncStart_extractsSnapshotTimestamp() {
        ChangeEvent event = makeFullSyncStartEvent(12345L);

        receiver.onFullSyncStart(event, driver, "node-1");

        assertEquals(12345L, receiver.getSnapshotTs());
    }

    @Test
    void isReceiving_duringReceiving_returnsTrue() {
        ChangeEvent event = makeFullSyncStartEvent(null);
        receiver.onFullSyncStart(event, driver, "node-1");

        assertTrue(receiver.isReceiving(),
                "Should be receiving after onFullSyncStart");
    }

    @Test
    void onFullSyncEnd_transitionsToCatchingUp() {
        // First enter RECEIVING state
        ChangeEvent startEvent = makeFullSyncStartEvent(null);
        receiver.onFullSyncStart(startEvent, driver, "node-1");

        // Then end full sync
        ChangeEvent endEvent = makeEvent(ChangeEventType.FULL_SYNC_END);
        receiver.onFullSyncEnd(endEvent);

        assertEquals(FullSyncReceiver.State.CATCHING_UP, receiver.getState());
    }

    @Test
    void onFullSyncEnd_isReceiving_returnsFalse() {
        ChangeEvent startEvent = makeFullSyncStartEvent(null);
        receiver.onFullSyncStart(startEvent, driver, "node-1");

        ChangeEvent endEvent = makeEvent(ChangeEventType.FULL_SYNC_END);
        receiver.onFullSyncEnd(endEvent);

        assertFalse(receiver.isReceiving(),
                "Should not be receiving in CATCHING_UP state");
    }

    @Test
    void onCatchUpComplete_returnsToIdle() {
        // Go through full lifecycle: IDLE -> PREPARING -> RECEIVING -> CATCHING_UP -> IDLE
        ChangeEvent startEvent = makeFullSyncStartEvent(null);
        receiver.onFullSyncStart(startEvent, driver, "node-1");

        ChangeEvent endEvent = makeEvent(ChangeEventType.FULL_SYNC_END);
        receiver.onFullSyncEnd(endEvent);

        receiver.onCatchUpComplete("node-1");

        assertEquals(FullSyncReceiver.State.IDLE, receiver.getState());
    }

    @Test
    void onCatchUpComplete_isReceiving_returnsFalse() {
        receiver.onCatchUpComplete("node-1");
        assertFalse(receiver.isReceiving());
    }

    private static ChangeEvent makeFullSyncStartEvent(Long snapshotTs) {
        EntityData entity = null;
        if (snapshotTs != null) {
            entity = new EntityData(
                    EntityType.NODE,
                    "elem-1",
                    List.of(),
                    Map.of("snapshotTs", snapshotTs),
                    null,
                    null,
                    null,
                    null
            );
        }
        return new ChangeEvent(
                "evt-1",
                ChangeEventType.FULL_SYNC_START,
                "neo4j",
                System.currentTimeMillis(),
                1L,
                "tx-1",
                entity,
                null
        );
    }

    private static ChangeEvent makeEvent(ChangeEventType type) {
        return new ChangeEvent(
                "evt-2",
                type,
                "neo4j",
                System.currentTimeMillis(),
                1L,
                "tx-1",
                null,
                null
        );
    }
}
