package com.neo4j.ha.agent.http.auth;

import at.favre.lib.crypto.bcrypt.BCrypt;
import com.neo4j.ha.common.config.HaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reads users from HaConfig.admin.ui.users at startup and verifies passwords
 * with bcrypt. Immutable after construction — config changes require Agent
 * restart (acceptable: the user table is operator-managed and ≤ 5 entries).
 */
public class UserStore {

    private static final Logger log = LoggerFactory.getLogger(UserStore.class);

    public record User(String username, String passwordHash, Role role) {}

    private final Map<String, User> users;

    public UserStore(HaConfig.UiConfig uiConfig) {
        Map<String, User> table = new ConcurrentHashMap<>();
        if (uiConfig != null && uiConfig.users() != null) {
            for (var u : uiConfig.users()) {
                if (u.username() == null || u.passwordHash() == null) continue;
                table.put(u.username(), new User(
                    u.username(),
                    u.passwordHash(),
                    Role.parse(u.role())
                ));
            }
        }
        this.users = Map.copyOf(table);
        log.info("UserStore loaded {} user(s)", users.size());
    }

    public Optional<User> verify(String username, String password) {
        if (username == null || password == null) return Optional.empty();
        User u = users.get(username);
        if (u == null) {
            // Run a dummy bcrypt verify on a fixed hash to keep response time roughly
            // constant whether or not the username exists (time-equalize, prevents
            // username enumeration via timing side-channel).
            BCrypt.verifyer().verify(password.toCharArray(), DUMMY_HASH);
            return Optional.empty();
        }
        var result = BCrypt.verifyer().verify(password.toCharArray(), u.passwordHash());
        return result.verified ? Optional.of(u) : Optional.empty();
    }

    public int size() {
        return users.size();
    }

    // Cost-10 hash of a random throwaway string. Used only to burn ~80ms of CPU
    // when the supplied username doesn't exist, so attacker timing observations
    // can't reveal whether a username is valid.
    private static final String DUMMY_HASH =
        "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy";
}
