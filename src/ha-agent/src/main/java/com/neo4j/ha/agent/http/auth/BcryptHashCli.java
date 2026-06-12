package com.neo4j.ha.agent.http.auth;

import at.favre.lib.crypto.bcrypt.BCrypt;

/**
 * Command-line tool to generate a bcrypt hash suitable for
 * {@code admin.ui.users[].passwordHash} in ha-agent.yml.
 *
 * <pre>
 * java -cp ha-agent.jar com.neo4j.ha.agent.http.auth.BcryptHashCli &lt;plaintext&gt;
 * </pre>
 *
 * <p>Cost factor is 10 (≈ 80 ms on commodity x86, recommended OWASP default).</p>
 */
public final class BcryptHashCli {

    private BcryptHashCli() {}

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: BcryptHashCli <plaintext-password>");
            System.exit(2);
        }
        String hash = BCrypt.withDefaults().hashToString(10, args[0].toCharArray());
        System.out.println(hash);
    }
}
