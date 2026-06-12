package com.neo4j.ha.agent.http.auth;

import at.favre.lib.crypto.bcrypt.BCrypt;
import com.neo4j.ha.common.config.HaConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class UserStoreTest {

    private static String hash(String pwd) {
        return BCrypt.withDefaults().hashToString(10, pwd.toCharArray());
    }

    private static HaConfig.UiConfig uiOf(HaConfig.UiUserConfig... users) {
        return new HaConfig.UiConfig(true, List.of(users), null, null);
    }

    @Test
    void verifyCorrectPassword() {
        var store = new UserStore(uiOf(
            new HaConfig.UiUserConfig("alice", hash("s3cret"), "admin")
        ));
        Optional<UserStore.User> u = store.verify("alice", "s3cret");
        assertTrue(u.isPresent());
        assertEquals(Role.ADMIN, u.get().role());
    }

    @Test
    void rejectWrongPassword() {
        var store = new UserStore(uiOf(
            new HaConfig.UiUserConfig("alice", hash("s3cret"), "admin")
        ));
        assertTrue(store.verify("alice", "wrong").isEmpty());
    }

    @Test
    void rejectUnknownUser() {
        var store = new UserStore(uiOf(
            new HaConfig.UiUserConfig("alice", hash("s3cret"), "admin")
        ));
        assertTrue(store.verify("nobody", "anything").isEmpty());
    }

    @Test
    void viewerRoleParsed() {
        var store = new UserStore(uiOf(
            new HaConfig.UiUserConfig("bob", hash("pwd"), "viewer")
        ));
        var u = store.verify("bob", "pwd");
        assertTrue(u.isPresent());
        assertEquals(Role.VIEWER, u.get().role());
        assertFalse(u.get().role().canWrite());
    }

    @Test
    void emptyConfigToleratedAsZeroUsers() {
        var store = new UserStore(null);
        assertEquals(0, store.size());
        assertTrue(store.verify("anyone", "anything").isEmpty());
    }
}
