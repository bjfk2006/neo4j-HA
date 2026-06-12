# Neo4j HA Agent — Web UI

Vue 3 + Element Plus + Vite. Lives outside `src/` so the JVM toolchain ignores
it; the Vite build outputs the bundle into
`../src/ha-agent/src/main/resources/static`, where Javalin serves it at `/`.

## Local development

```bash
cd ui
npm install
npm run dev     # http://localhost:5173, proxies /api → http://localhost:8080
```

You need an agent running on `localhost:8080` with `admin.ui.enabled: true`
and at least one user configured in `admin.ui.users` for login to work.

## Production build

```bash
npm run build   # outputs to ../src/ha-agent/src/main/resources/static
```

Then build the agent JAR as usual; the bundle is on the classpath.

## Via Maven (single command)

```bash
mvn -Pwith-ui -pl src/ha-agent -am clean package
```

The `with-ui` profile downloads a sandboxed Node, runs `npm install` and
`npm run build`, then packages everything into one jar.

## Generating a password hash for `admin.ui.users[].passwordHash`

```bash
# After mvn package, with the agent jar on classpath:
java -cp src/ha-agent/target/ha-agent.jar \
  com.neo4j.ha.agent.http.auth.BcryptHashCli 'my-plaintext-password'
```

Paste the `$2a$10$...` output into the YAML.
