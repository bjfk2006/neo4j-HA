# Neo4j HA Java 程序编写与打包操作指南

> 适用范围：当前仓库 `neo4j-HA` 全部 Java 模块  
> 构建工具：Apache Maven（多模块项目）  
> JVM 版本：**Java 17（LTS）**

---

## 1. JVM 版本要求

| 项目 | 版本 |
|------|------|
| **JDK** | 17（LTS），编译源码和目标字节码均为 17 |
| **Maven** | 3.8.6+（需支持 `maven-compiler-plugin 3.13.0`） |

父 POM 中的关键配置：

```xml
<properties>
    <java.version>17</java.version>
    <maven.compiler.source>${java.version}</maven.compiler.source>
    <maven.compiler.target>${java.version}</maven.compiler.target>
</properties>
```

> **为什么选择 Java 17？**
> - Neo4j Java Driver 5.x 最低要求 JDK 17
> - Javalin 6.x、Jedis 5.x 等核心依赖均要求 JDK 17+
> - Java 17 是当前活跃的 LTS 版本，兼具稳定性与现代语言特性（sealed classes、records、pattern matching 等）

### 1.1 验证 JDK 版本

```bash
java -version
# 输出应为 openjdk version "17.x.x" 或类似

mvn -version
# Maven home、Java version 行应显示 17
```

### 1.2 推荐 JDK 发行版

| 发行版 | 适用场景 |
|--------|---------|
| Eclipse Temurin 17 | 通用首选，社区活跃 |
| Amazon Corretto 17 | AWS 环境推荐 |
| Oracle JDK 17 | 需要商业支持时使用 |

---

## 2. 项目模块结构

```
neo4j-HA/
├── pom.xml                          # 父 POM（聚合 + 依赖管理）
├── src/
│   ├── common/                      # neo4j-ha-common：公共模型、客户端、工具
│   │   └── pom.xml
│   ├── cdc-collector/               # neo4j-ha-cdc-collector：CDC 变更捕获
│   │   └── pom.xml
│   ├── sync-applier/                # neo4j-ha-sync-applier：数据同步回放
│   │   └── pom.xml
│   └── ha-agent/                    # neo4j-ha-agent：集中式 HA 管理（主入口）
│       └── pom.xml
├── config/
│   └── agent/ha-agent.yml           # HA Agent 运行时配置
└── docker/
    ├── .env(.example)               # 容器环境变量
    └── neo4j/test-compose.yml       # 当前联调/部署使用的 Compose 文件
```

模块依赖关系：

```
ha-agent ──► cdc-collector ──► common
         └─► sync-applier ──► common
```

`ha-agent` 是最终可执行模块，主类为 `com.neo4j.ha.agent.HaAgent`。

---

## 3. 开发环境搭建

### 3.1 安装 JDK 17

**macOS（Homebrew）：**

```bash
brew install openjdk@17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

**Linux（apt）：**

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

**Linux（yum/dnf）：**

```bash
sudo yum install -y java-17-openjdk-devel
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
```

建议将 `JAVA_HOME` 写入 `~/.bashrc` 或 `~/.zshrc`。

### 3.2 安装 Maven

```bash
# macOS
brew install maven

# Linux（手动安装）
# 建议先设置版本变量（按 Apache 官方最新 3.9.x 调整）
MAVEN_VERSION=3.9.14 wget "https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
  || wget "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
sudo tar xzf "apache-maven-${MAVEN_VERSION}-bin.tar.gz" -C /opt
export PATH="/opt/apache-maven-${MAVEN_VERSION}/bin:$PATH"
```

### 3.3 IDE 配置建议

- **IntelliJ IDEA**：打开根目录 `pom.xml` 作为 Maven 项目导入，SDK 选择 JDK 17
- **VS Code**：安装 Extension Pack for Java，在 `settings.json` 中配置 `java.configuration.runtimes` 指向 JDK 17

---

## 4. 编译与打包

### 4.1 全量编译（推荐）

从项目根目录执行：

```bash
cd /path/to/neo4j-HA
mvn clean compile
```

### 4.2 跳过测试打包

```bash
mvn clean package -DskipTests
```

构建产物位于各模块的 `target/` 目录：

```
src/common/target/neo4j-ha-common-1.0.0-SNAPSHOT.jar
src/cdc-collector/target/neo4j-ha-cdc-collector-1.0.0-SNAPSHOT.jar
src/sync-applier/target/neo4j-ha-sync-applier-1.0.0-SNAPSHOT.jar
src/ha-agent/target/neo4j-ha-agent-1.0.0-SNAPSHOT.jar
```

### 4.3 执行全部测试

```bash
mvn clean verify
```

### 4.4 单模块编译

```bash
# 仅编译 common 模块
mvn clean compile -pl src/common

# 编译 ha-agent 及其依赖模块
mvn clean compile -pl src/ha-agent -am
```

`-pl` 指定模块，`-am`（also-make）自动编译上游依赖。

### 4.5 安装到本地仓库

```bash
mvn clean install -DskipTests
```

将所有模块 JAR 安装至 `~/.m2/repository`，方便其他项目引用。

---

## 5. 当前 Docker 部署（最新）

### 5.1 当前 Docker 部署配置（与代码一致）

当前联调流程使用以下 3 个配置文件：

| 文件 | 作用 | 关键项 |
|------|------|--------|
| `docker/neo4j/test-compose.yml` | 编排 Neo4j/HAProxy/HA Agent 容器 | Neo4j 插件、HAProxy socket 挂载、HA Agent 启动命令与 classpath |
| `config/agent/ha-agent.yml` | HA Agent 业务配置 | Neo4j 节点地址、Redis、HAProxy socket、CDC/Failover 参数 |
| `docker/.env`（由 `.env.example` 复制） | 容器启动环境变量 | `NEO4J_PASSWORD`、`ADMIN_TOKEN`、`REDIS_PASSWORD`、内存参数 |

> 注意：`test-compose.yml` 中 `ha-agent` 的 `REDIS_HOST`/`REDIS_PORT` 当前固定为 `172.19.0.11:6379`，优先级高于 `docker/.env` 里的同名变量。

### 5.2 Docker 启动操作流程

**方式一：一键启动脚本（推荐）**

```bash
cd /path/to/neo4j-HA

# 0) 首次准备
cp -n docker/.env.example docker/.env   # 然后修改密码等参数
rm -rf src/ha-agent/target/dependency
mvn clean package -DskipTests
mvn -pl src/sync-applier,src/ha-agent,src/cdc-collector,src/common -am -DskipTests clean install
mvn clean package -pl src/sync-applier,src/ha-agent,src/cdc-collector,src/common -am -DskipTests
mvn clean install -DskipTests
mvn -pl src/ha-agent dependency:copy-dependencies -DincludeScope=runtime
cp config/agent/ha-agent.yml /opt/ha-agent/ha-agent.yml

# 1) 一键启动（清空数据 → 启动全部服务）
bash docker/init.sh
bash docker/init-3node.sh
```

脚本内部自动完成：加载 `.env` → 清空旧数据 → 清空 Redis HA 状态 → 创建 bind mount 目录 → 按顺序启动 Neo4j → 轮询健康检查 → 启动 HAProxy → 启动 HA Agent。

**方式二：手动分步启动**

```bash
cd /path/to/neo4j-HA

cp -n docker/.env.example docker/.env
mvn clean package -DskipTests -pl src/ha-agent -am
mvn -pl src/ha-agent dependency:copy-dependencies -DincludeScope=runtime
mkdir -p /opt/ha-agent && cp config/agent/ha-agent.yml /opt/ha-agent/ha-agent.yml

mkdir -p \
  /opt/neo4j-node1/{data,logs,import,plugins} \
  /opt/neo4j-node2/{data,logs,import,plugins} \
  /opt/haproxy-1/haproxy-1-socket \
  /opt/haproxy-2/haproxy-2-socket \
  /opt/ha-agent/buffer

docker compose -f docker/neo4j/test-compose.yml --env-file docker/.env up -d neo4j-primary neo4j-standby
# 等待 healthy 后继续
docker compose -f docker/neo4j/test-compose.yml --env-file docker/.env up -d haproxy-1 haproxy-2
docker compose -f docker/neo4j/test-compose.yml --env-file docker/.env up -d ha-agent
```

### 5.3 功能验证（Smoke Test）

启动完成后，运行一键 HA 验证脚本：

```bash
NEO4J_PASSWORD=<你的密码> ADMIN_TOKEN=<你的token> bash scripts/deploy/ha-smoke-test.sh
```

验证覆盖项：Agent 健康端点、Prometheus 指标、集群状态、CDC 主→备复制、Switchover 切换后复制。

### 5.4 验证与故障排查

```bash
# 查看服务状态
docker compose -f docker/neo4j/test-compose.yml --env-file docker/.env ps

# 查看 HA Agent 日志
docker compose -f docker/neo4j/test-compose.yml --env-file docker/.env logs -f ha-agent

# 查看 Neo4j 健康状态（容器级）
docker inspect neo4j-primary --format '{{json .State.Health}}'
docker inspect neo4j-standby --format '{{json .State.Health}}'
```

若 HA Agent 日志出现 `role is: FOLLOWER`（触发器安装阶段）：

1. 当前代码已改为在 `system` 数据库会话执行 `apoc.trigger.install`，并指定目标业务库为 `neo4j`
2. 启动初期会自动重试（10 次，每次间隔 3s）
3. 若仍失败，Agent 以降级模式继续运行（不会因触发器安装失败直接退出）

若 HA Agent 日志出现 `WRONGPASS invalid username-password pair`：

1. 检查 `docker/.env` 中 `REDIS_PASSWORD` 是否与实际 Redis 密码一致
2. 若 Redis 无密码，将 `REDIS_PASSWORD=` 设为空
3. 注意 `test-compose.yml` 中 `ha-agent` 的 `REDIS_HOST` 硬编码为 `172.19.0.11`，确认与实际 Redis 地址一致

---

## 6. JVM 运行参数建议

### 6.1 通用生产参数

```bash
java \
  -Xms1g -Xmx2g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=/var/log/neo4j-ha/heapdump.hprof \
  -Dfile.encoding=UTF-8 \
  -Duser.timezone=Asia/Singapore \
  -jar ha-agent.jar
```

### 6.2 参数说明

| 参数 | 说明 |
|------|------|
| `-Xms` / `-Xmx` | 堆内存初始值与上限，建议设为相同值避免运行时调整 |
| `-XX:+UseG1GC` | G1 垃圾收集器，适合低延迟要求的服务端应用 |
| `-XX:MaxGCPauseMillis=200` | GC 停顿目标 200ms |
| `-XX:+HeapDumpOnOutOfMemoryError` | OOM 时自动 dump 堆快照，方便事后分析 |
| `-Dfile.encoding=UTF-8` | 统一字符编码 |
| `-Duser.timezone` | 统一时区，与容器环境变量 `TZ` 保持一致 |

### 6.3 内存容量规划

| 部署场景 | 推荐堆内存 |
|----------|-----------|
| 开发/测试 | 512MB ~ 1GB |
| 生产（≤3 节点集群） | 1GB ~ 2GB |
| 生产（大规模同步、高 CDC 吞吐） | 2GB ~ 4GB |

---

## 7. 核心依赖清单

以下为父 POM 中统一管理的主要依赖版本：

| 依赖 | 版本 | 用途 |
|------|------|------|
| Neo4j Java Driver | 5.18.0 | 连接 Neo4j 节点 |
| Jedis | 5.1.0 | 连接 Redis（Stream 消息队列） |
| Jackson Databind | 2.17.0 | JSON 序列化/反序列化 |
| Jackson YAML | 2.17.0 | YAML 配置解析 |
| Micrometer Prometheus | 1.12.4 | Prometheus 指标暴露 |
| Javalin | 6.1.3 | 轻量级 HTTP 服务（Admin API） |
| SLF4J | 2.0.12 | 日志门面 |
| Logback | 1.5.3 | 日志实现 |
| Logstash Encoder | 7.4 | 结构化 JSON 日志 |
| JUnit 5 | 5.10.2 | 单元测试框架 |
| Mockito | 5.11.0 | Mock 测试 |

更新依赖版本时，仅修改根 `pom.xml` 的 `<properties>` 节即可，各子模块自动继承。

---

## 8. 常见问题排查

### 8.1 编译报错：`source/target 不兼容`

```
[ERROR] Source option 17 is not supported. Use 11 or earlier.
```

**原因**：当前 `JAVA_HOME` 指向 JDK 11 或更低版本。  
**解决**：安装 JDK 17 并确认 `JAVA_HOME` 和 `PATH` 正确指向。

### 8.2 依赖下载失败

```bash
# 检查 Maven 仓库配置
cat ~/.m2/settings.xml

# 使用阿里云镜像（国内环境推荐）
# 在 settings.xml 中添加：
```

```xml
<mirror>
    <id>aliyun</id>
    <mirrorOf>central</mirrorOf>
    <url>https://maven.aliyun.com/repository/central</url>
</mirror>
```

### 8.3 `ClassNotFoundException` 启动失败

标准 JAR 不包含依赖。确认是否已配置 `maven-shade-plugin` 打 Fat JAR，或使用 `-cp` 显式指定 classpath：

```bash
java -cp "lib/*:ha-agent.jar" com.neo4j.ha.agent.HaAgent
```

### 8.4 Docker 镜像中 JVM 内存超限被 OOM Kill

容器 `memory` 限制需大于 JVM 堆（`-Xmx`）+ 非堆（约 256~512MB）。例如 `-Xmx2g` 时容器建议分配 ≥ 3GB。

---

## 9. 开发约定

- 代码规范遵循项目根目录的格式化配置（如有）
- 新增类需包含 SLF4J Logger 且使用结构化日志
- 公共 API 变更需同步更新 `common` 模块
- 所有变更需通过 `mvn verify` 确保测试通过后方可提交

---

## 10. Web UI 构建增量（v1.1+）

> 适用于启用 ha-agent 内置 Web 管理 UI（`admin.ui.enabled: true`）的场景。**未启用 UI 时，第 1–9 节流程完全适用，无需阅读本节。**
>
> UI 静态资源以 classpath 资源形式嵌入到 `neo4j-ha-agent-*.jar` 的 `/static/` 路径下，由 Javalin 在原 8080 端口提供服务——**docker compose 编排、bind mount 布局、`init.sh` 流程均无变化**。本节只描述在第 §4 / §5 流程之上插入的几处增量。

### 10.1 新增工具链与依赖

| 工具/依赖 | 版本 | 用途 | 何时需要 |
|---|---|---|---|
| Node.js | 20.x（LTS） | 跑 Vite 构建 | 仅当 `admin.ui.enabled: true` |
| npm | 10.x（随 Node 20） | 拉前端依赖 | 同上 |
| `at.favre.lib:bcrypt` | 0.10.2 | 后端 bcrypt 校验 + `BcryptHashCli` 工具 | 已自动加入 `src/ha-agent/pom.xml`，无需手动配置 |
| Vue 3 / Element Plus / ECharts / Pinia / vue-router | 见 `ui/package.json` | 前端运行时 | 由 npm 自动拉取 |

验证 Node：

```bash
node -v       # 应为 v20.x
npm -v        # 应为 10.x
```

如系统未装 Node 但希望"一条命令完成构建"，可使用 §10.3 的 `with-ui` Maven profile，由 `frontend-maven-plugin` 自动下载沙箱 Node。

### 10.2 目录与产物布局

```
neo4j-HA/
├── ui/                                          # 新增：前端源码
│   ├── package.json
│   ├── vite.config.js                           # 输出路径写死指向后端 resources/static
│   ├── index.html
│   └── src/{main.js, App.vue, router.js, views/, components/, stores/, api/}
└── src/ha-agent/src/main/
    ├── java/com/neo4j/ha/agent/http/auth/       # 新增：UI 鉴权后端
    │   ├── UserStore / SessionManager / RateLimiter / AuthFilter
    │   ├── AuthController / AuditController / MetricsSummaryController
    │   └── BcryptHashCli                        # 密码 hash CLI 工具
    └── resources/static/                        # ⚠ Vite 构建产物落地点
        ├── index.html                           # npm run build 后被覆盖
        └── assets/{*.js, *.css}                 # 同上
```

最终产物仍是单个 `src/ha-agent/target/neo4j-ha-agent-1.0.0-SNAPSHOT.jar`——UI bundle 已在它内部 `BOOT-INF/classes/static/` 路径下。

### 10.3 构建流程：在第 §5.2 "首次准备" 块之上的两种增量方式

**方式 A —— 手工 npm build（最透明、推荐首次）**

在原指南 §5.2 方式一的 `rm -rf src/ha-agent/target/dependency` **之前**插入：

```bash
cd ui
npm install --no-audit --no-fund                 # 首次或 package.json 变更时
npm run build                                    # 输出到 ../src/ha-agent/src/main/resources/static/
cd ..
```

完成后按原指南执行剩余 mvn 命令，Vite 的产物会被 `maven-jar-plugin` 一并打入 jar。

**方式 B —— 用 Maven `with-ui` profile 一条命令搞定**

把原指南 §5.2 方式一中的第一行 `mvn clean package -DskipTests` 替换为：

```bash
mvn -Pwith-ui -pl src/ha-agent -am clean package -DskipTests
```

该 profile 在 `src/ha-agent/pom.xml` 已配置，会自动：
1. 下载 Node v20.12.2 到 `ui/.node-cache/`（首次约 30MB，需联网）
2. 跑 `npm install`
3. 跑 `npm run build`
4. 编译 Java + 打 jar

剩余的 `dependency:copy-dependencies`、`install`、`cp ha-agent.yml` 步骤完全照旧。

> ⚠ 现有 `Dockerfile`（`docker/ha-agent/Dockerfile`）默认不带 `-Pwith-ui`。要让 Docker 镜像内带 UI，需在 Dockerfile 第 7 行的 `mvn ... clean install` 加 `-Pwith-ui`，并在第 6 行 `COPY src ./src` 后补一行 `COPY ui ./ui`。

### 10.4 启用 UI：配置 + 生成密码 hash

仅修改 `config/agent/ha-agent.yml` 中 `admin.ui` 块。**必须在第 §5.2 的 `cp config/agent/ha-agent.yml /opt/ha-agent/ha-agent.yml` 之前**编辑完成，否则容器读到旧配置。

#### 10.4.1 用 `BcryptHashCli` 生成密码 hash

需先完成第 §5.2 中的 `mvn ... clean package` + `dependency:copy-dependencies` 两步（保证 jar 与依赖目录就绪）。

```bash
cd /path/to/neo4j-HA

java -cp "src/ha-agent/target/neo4j-ha-agent-1.0.0-SNAPSHOT.jar:src/ha-agent/target/dependency/*" \
  com.neo4j.ha.agent.http.auth.BcryptHashCli 'YourStr0ngPassword'
```

输出形如：

```
$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy
```

每个账号执行一次。**不要在生产环境的 shell history 里留下明文密码**：用 `read -s pw && java -cp ... BcryptHashCli "$pw" && unset pw` 或临时调高 `HISTCONTROL=ignorespace` 后命令前加空格。

#### 10.4.2 编辑 `ha-agent.yml` 的 `admin.ui` 块

```yaml
admin:
  port: 8080
  auth:
    type: "token"
    token: "${ADMIN_TOKEN}"
  ui:
    enabled: true                       # 默认 false，改为 true 启用 UI
    users:
      - username: "admin"
        passwordHash: "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"
        role: "admin"                   # 'admin' 全权 / 'viewer' 只读
      # 可选：只读账号
      # - username: "viewer"
      #   passwordHash: "$2a$10$..."
      #   role: "viewer"
    session:
      ttl: "8h"                         # session 生存期
      maxPerUser: 3                     # 同账号并发上限
    rateLimit:
      maxFailuresPerMinute: 5           # 触发锁定的失败次数阈值
      lockDuration: "10m"               # 锁定时长
```

校验：`ConfigValidator` 会在启动时校验 `passwordHash` 必须以 `$2` 开头、`role` 必须是 `admin` 或 `viewer`，错误会在 Agent 启动日志的 `Configuration errors: [...]` 行报出并退出。

#### 10.4.3 YAML 编辑陷阱（必读，避免启动失败）

YAML 解析对**缩进 / 冒号 / 列表语法**极度敏感，Jackson + SnakeYAML 的报错信息含糊（典型 `expected <block end>, but found '<block sequence start>'`），一旦写错容易反复试错。下面是真实踩坑案例的合并清单，**编辑 `admin.ui` 时逐条对照**：

##### ❶ `enabled` 必须是布尔值 `true` / `false`

```yaml
# ❌ 错误：写成单词、字符串、缺值
enabled: ui          # SnakeYAML 把后续行视为同 mapping 的下个 key，整块结构错位
enabled: yes         # YAML 1.1 接受，但 Jackson 不一定接受，避免歧义
enabled:             # 缺值 → 解析为 null，等价于 false 但可读性差

# ✅ 正确
enabled: true
```

##### ❷ `users: []`（空数组）与 `- username:`（列表项）不能并存

```yaml
# ❌ 错误：既声明空数组又紧跟列表项
users: []
   - username: "admin"      # 解析器已认定 users 是空数组，下面这行被当成新 mapping 项
     passwordHash: "..."

# ✅ 正确（要用户）
users:
  - username: "admin"
    passwordHash: "..."
    role: "admin"

# ✅ 正确（不要用户，但保持配置块完整）
users: []
```

##### ❸ 列表项的缩进必须是「父 key 缩进 + 2 空格」

```yaml
# ❌ 错误：列表项相对 users: 缩了 3 个空格而不是 2
users:
   - username: "admin"      # ← 3 空格
     passwordHash: "..."

# ✅ 正确：相对 users: 缩 2 空格，`- ` 后再有 1 空格
users:
  - username: "admin"       # ← 2 空格 + "- " + key
    passwordHash: "..."     # ← 4 空格（与 username 对齐）
    role: "admin"
```

##### ❹ 不要把字段误注释掉

```yaml
# ❌ 错误：role 字段被 # 注释掉，会触发 ConfigValidator 校验失败
- username: "admin"
  passwordHash: "..."
#   role: "admin"           # ← 注释掉等于没配，但 ConfigValidator 仍会通过（默认 admin），易迷惑

# ✅ 正确：显式写出 role
- username: "admin"
  passwordHash: "..."
  role: "admin"
```

##### ❺ 缩进规则速查表

```yaml
admin:                      # 顶级 key，0 空格
  port: 8080                # admin 子 key，2 空格
  ui:                       # 2 空格
    enabled: true           # ui 子 key，4 空格
    users:                  # 4 空格
      - username: "admin"   # 列表项，6 空格 + "- " + key
        passwordHash: "..." # 列表项内字段，8 空格（与 username 对齐）
        role: "admin"       # 8 空格
    session:                # 回到 ui 子 key 层级，4 空格
      ttl: "8h"             # session 子 key，6 空格
      maxPerUser: 3
```

##### ❻ 编辑后必跑的本地语法校验

**不要直接 `docker compose up` 试错**——容器启动失败定位耗时。改完先用 Python 离线 parse：

```bash
python3 -c 'import yaml; yaml.safe_load(open("/opt/ha-agent/ha-agent.yml"))' \
  && echo "YAML OK"
```

无输出 + 看到 `YAML OK` 才进入下一步；任何打印输出（含行号）都意味着 yml 仍有问题，**继续修复直到通过为止**。

如果系统没装 Python，用 yq（如已安装）或 docker 临时跑：

```bash
docker run --rm -v /opt/ha-agent/ha-agent.yml:/c.yml mikefarah/yq:4 e '.' /c.yml > /dev/null \
  && echo "YAML OK"
```

##### ❼ 启动后必看的关键日志行

修完 yml 重启容器后，前 20 行日志应能看到三条关键确认：

```
INFO  ConfigLoader     - Configuration loaded from /app/config/ha-agent.yml
INFO  UserStore        - UserStore loaded N user(s)              ← N 不为 0
INFO  AdminHttpServer  - Admin HTTP server started on port 8080 (ui=true)
```

任何一条缺失或显示 `ui=false` / `UserStore loaded 0 user(s)` 都说明 yml 没生效（最常见是改了仓库版本忘了 `cp` 到 `/opt/ha-agent/`）。

#### 10.4.4 拷到 bind mount 目录（与原指南完全一致）

```bash
cp config/agent/ha-agent.yml /opt/ha-agent/ha-agent.yml
```

### 10.5 启动与验证（与原指南 §5.2 / §5.3 一致）

```bash
bash docker/init.sh
bash docker/init-3node.sh

# 跑官方 smoke test（与有/无 UI 无关）
NEO4J_PASSWORD=<密码> ADMIN_TOKEN=<token> bash scripts/deploy/ha-smoke-test.sh
```

UI 专属验证：

```bash
# 1. 健康端点（无需 auth）
curl -s http://<agent-host>:8080/health
# → {"status":"UP"}

# 2. 浏览器
open http://<agent-host>:8080
# 自动跳转到 /login → 输入 admin / YourStr0ngPassword → /dashboard 展示拓扑 + 指标

# 3. 用 cookie 登录后调 /api/me
curl -s -c /tmp/c -X POST http://<agent-host>:8080/api/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"YourStr0ngPassword"}'
curl -s -b /tmp/c http://<agent-host>:8080/api/me

# 4. 原有 curl 脚本（X-Admin-Token / Authorization: Bearer）保持可用
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://<agent-host>:8080/api/cluster/status
```

### 10.6 开发模式（本机热重载，不打 jar）

仅适用于改 UI 时的快速迭代：

```bash
# 终端 1：启动 Agent（按 §5.2 流程，admin.ui.enabled 必须 true）
bash docker/init.sh && bash docker/init-3node.sh

# 终端 2：Vite dev server，自动 proxy /api → localhost:8080
cd ui && npm run dev
# 打开 http://localhost:5173；改前端代码自动热刷新
```

### 10.7 故障排查清单（UI 专属）

| 现象 | 原因 | 处理 |
|---|---|---|
| `mvn compile` 报 `XRangeParams 找不到符号` | Jedis API 漂移 | 已在 `AuditController` 使用 positional 签名规避；若仍报错，把 `xrange/xrevrange(key, start, end, count)` 改为 `xrange/xrevrange(key, start, end)` 然后 Java 端 `subList(0, limit)` |
| 启动日志 `MarkedYAMLException: expected <block end>, but found '<block sequence start>'` 或 `while parsing a block mapping` | yml 缩进 / 列表语法错（最常见：`users: []` 后又紧跟 `- username:`；列表项缩进多/少一个空格；`enabled` 写成非布尔值） | 严格按 §10.4.3 的缩进表与避坑清单对照修复，并用 `python3 -c 'import yaml; yaml.safe_load(open("/opt/ha-agent/ha-agent.yml"))'` 离线校验通过后再启容器 |
| 启动日志 `Configuration errors: [admin.ui.users[].passwordHash must be a bcrypt hash...]` | yml 中 hash 缺失或未引号包裹 | hash 整段必须以 `$2` 开头并放在双引号内 |
| 启动日志 `Configuration errors: [admin.ui.enabled is true but admin.ui.users is empty]` | `enabled: true` 但 `users:` 为空数组或被全部注释掉 | 至少配 1 个 user 并去掉 `#` 注释；或把 `enabled` 改回 `false` 关 UI |
| 启动日志 `UserStore loaded 0 user(s)` 且 `ui=true` | 编辑了仓库版本但忘了 `cp` 到 `/opt/ha-agent/ha-agent.yml`（bind mount 还指向旧文件） | 重新 `cp config/agent/ha-agent.yml /opt/ha-agent/ha-agent.yml` 再 `restart ha-agent` |
| 浏览器打开 `/` 看到"UI not built yet" 占位页 | 前端未构建，jar 里仍是占位 index.html | 执行 §10.3 方式 A 或 B 后重打 jar |
| 登录返回 423 `Too many failed attempts` | 触发 5/min IP 锁 | 等 `lockDuration` 过期，或重启 Agent（内存计数清空） |
| 浏览器登录成功但下一秒跳回 `/login` | Cookie 未生效（端口/域名跨域、HTTPS-only 标记不匹配） | 使用同源访问；生产应在反代终止 HTTPS 后由反代设 `Secure` 标记或在 Agent 启动时通过 `AuthController.setSecureCookie(true)`（v1.2 计划） |
| 点 Failover 弹 403 | 当前账号 role 为 `viewer` | 用 `admin` 账号登录，或在 yml 把 user 的 role 改为 `admin` |
| Dashboard 的 `fencingToken` / `streamLen` 显示 `—` | Redis 不可达 | 检查 `docker/.env` 中 `REDIS_PASSWORD` / `REDIS_HOST`；不阻塞登录，但指标缺失 |

### 10.8 鉴权双轨速查

| 客户端 | 推荐鉴权方式 | 凭据来源 |
|---|---|---|
| 浏览器 UI | Cookie `ha_session` | `POST /api/login` 后由 Set-Cookie 颁发 |
| curl / 运维脚本 | `X-Admin-Token: <token>` 或 `Authorization: Bearer <token>` | `docker/.env` 的 `ADMIN_TOKEN` |
| 监控抓取 | 无（`/health` `/metrics` 不需要 auth） | — |

两种方式**完全独立、可并存**；任何一种通过都视为已认证。Token 通道始终拥有 ADMIN 权限，Cookie 通道则按 user 的 `role` 决定。

### 10.9 新增的 ha-agent 指标（接入 Prometheus）

| Metric | 类型 | 说明 |
|---|---|---|
| `ha_ui_login_total{result="success\|failure\|locked"}` | Counter | UI 登录尝试结果 |
| `ha_ui_session_active` | Gauge | 当前活跃 session 数 |
| `ha_ui_api_requests_total{path, method, status}` | Counter | API 请求量（含 path label，可分钻取） |
| `ha_ui_api_duration_seconds{path}` | Timer | API 端到端延迟 |

告警建议：

| 指标 | 阈值 | 含义 |
|---|---|---|
| `rate(ha_ui_login_total{result="failure"}[1m])` | > 10/min | 暴力破解可能 |
| `rate(ha_ui_login_total{result="locked"}[1m])` | > 1/min | 持续撞库 |
| `histogram_quantile(0.95, ha_ui_api_duration_seconds_bucket{path="/api/cluster/status"})` | > 1s | Agent 自身慢 |

### 10.10 一页纸：完整的打包 → 启动流程（启用 UI 版）

```bash
cd /path/to/neo4j-HA

# 0) 准备 .env（仅首次）
cp -n docker/.env.example docker/.env
# 编辑 docker/.env：NEO4J_PASSWORD / ADMIN_TOKEN / REDIS_PASSWORD

# 1) 构建前端（方式 A）
cd ui && npm install --no-audit --no-fund && npm run build && cd ..

# 2) 构建后端（与原指南 §5.2 完全一致）
rm -rf src/ha-agent/target/dependency
mvn clean package -DskipTests
mvn -pl src/sync-applier,src/ha-agent,src/cdc-collector,src/common -am -DskipTests clean install
mvn clean package -pl src/sync-applier,src/ha-agent,src/cdc-collector,src/common -am -DskipTests
mvn clean install -DskipTests
mvn -pl src/ha-agent dependency:copy-dependencies -DincludeScope=runtime

# 3) 生成密码 hash
java -cp "src/ha-agent/target/neo4j-ha-agent-1.0.0-SNAPSHOT.jar:src/ha-agent/target/dependency/*" \
  com.neo4j.ha.agent.http.auth.BcryptHashCli 'YourStr0ngPassword'
# 把输出粘贴到 config/agent/ha-agent.yml 的 admin.ui.users[].passwordHash，
# 并把 admin.ui.enabled 改为 true。

# 4) 拷配置到 bind mount 目录
cp config/agent/ha-agent.yml /opt/ha-agent/ha-agent.yml

# 5) 一键启动
bash docker/init.sh
bash docker/init-3node.sh

# 6) 验证
NEO4J_PASSWORD=<密码> ADMIN_TOKEN=<token> bash scripts/deploy/ha-smoke-test.sh
open http://<agent-host>:8080
```

如以上步骤 2 中任何一步 mvn 编译失败、错误指向新加的 `auth/` 包或 `AuditController`，请把 `mvn -pl src/ha-agent -am compile 2>&1 | tail -30` 的输出贴出来排查；常见症结见 §10.7。

---

## 11. Neo4j 插件离线化配置（避免每次启动重新下载 GDS）

> 与 §10 Web UI 无关，但很多人首次启动 `deploy-test.yml` 都会踩到。建议**首次部署前**就按本节配置好，避免外网依赖。

### 11.1 现象

Neo4j 容器启动日志反复出现：

```
Fetching versions.json for Plugin 'graph-data-science' from https://graphdatascience.ninja/versions.json
Installing Plugin 'graph-data-science' from https://graphdatascience.ninja/neo4j-graph-data-science-2.13.9.jar
```

每次 `docker compose up` / `restart` 都重新拉一次，造成：

- 启动时间增加 10–60s（取决于网络）
- 离线 / 内网环境直接失败：`Could not resolve host: graphdatascience.ninja`
- `https://graphdatascience.ninja/` 偶发抽风 → 容器启动失败 → 集群不可用

### 11.2 根因

Neo4j 官方镜像的 entrypoint 脚本在每次容器启动时按 `NEO4J_PLUGINS` 环境变量**重新跑一遍** `install_plugin` 流程：

| 插件 | 是否随官方镜像打包 | 启动行为 |
|---|---|---|
| **APOC** | ✅ 镜像内 `/var/lib/neo4j/labs/apoc-*-core.jar` 自带 | 仅 `cp` 到 `/plugins/`，**不走网络** |
| **GDS**（graph-data-science） | ❌ 不在镜像中（license + 体积原因） | 拉 `versions.json` + 下载 jar |

GDS 的 install 脚本**强制覆盖** `/plugins/` 里同名 jar，**不做"已存在就跳过"判断**——所以即便 bind mount 的 `/opt/neo4j-*/plugins/` 里已有 jar，下次启动仍会重新下载覆盖。这是官方镜像多年的设计行为，**不是 bug，无法通过配置项关闭**。唯一根治办法是从 `NEO4J_PLUGINS` 列表移除。

### 11.3 方案 A：预下载 jar + 从 `NEO4J_PLUGINS` 移除（推荐，10 分钟搞定）

```bash
# 1) 一次性下载 GDS jar 到所有节点的 plugins bind mount
GDS_VERSION=2.13.9
curl -L -o /opt/neo4j-1/plugins/neo4j-graph-data-science.jar \
  https://graphdatascience.ninja/neo4j-graph-data-science-${GDS_VERSION}.jar
cp /opt/neo4j-1/plugins/neo4j-graph-data-science.jar \
   /opt/neo4j-2/plugins/neo4j-graph-data-science.jar
# 三节点部署再加：
# cp ... /opt/neo4j-3/plugins/neo4j-graph-data-science.jar

# 2) 验证 jar 完整（不是被防火墙篡改成的 HTML 错误页）
file /opt/neo4j-1/plugins/neo4j-graph-data-science.jar
# 应输出：Java archive data (JAR)
ls -la /opt/neo4j-1/plugins/neo4j-graph-data-science.jar
# 大小应 ≥ 100 MB
```

修改 compose 文件（`deploy-test.yml` / `test-compose.yml` 中**所有 Neo4j 服务的 env 都改**）：

```yaml
# 修改前
NEO4J_PLUGINS: '["apoc", "graph-data-science"]'

# 修改后（apoc 保留——走本地 labs 目录，零网络代价）
NEO4J_PLUGINS: '["apoc"]'
```

`NEO4J_dbms_security_procedures_unrestricted` 和 `NEO4J_dbms_security_procedures_allowlist` 里的 `gds.*` **保留不变**——Neo4j 启动时自动加载 plugins 目录里所有 jar，安全白名单决定它们能否被调用。

重启：

```bash
docker compose -f docker/neo4j/deploy-test.yml --env-file docker/.env down
docker compose -f docker/neo4j/deploy-test.yml --env-file docker/.env up -d
docker compose -f docker/neo4j/deploy-test.yml --env-file docker/.env logs --tail 30 neo4j-primary
```

成功标志：启动日志**不再出现** `Fetching versions.json` / `Installing Plugin 'graph-data-science' from https://...`，只剩 APOC 的本地 cp。

### 11.4 方案 B：构建自定义 Neo4j 镜像（生产 / 离线环境推荐）

适合严格离线、镜像产物需要纳入 OS 镜像仓库管理的场景：

```dockerfile
# docker/neo4j/Dockerfile
FROM neo4j:5.26-community
ARG GDS_VERSION=2.13.9
RUN curl -fsSL -o /var/lib/neo4j/plugins/neo4j-graph-data-science.jar \
      https://graphdatascience.ninja/neo4j-graph-data-science-${GDS_VERSION}.jar
```

```bash
docker build -f docker/neo4j/Dockerfile \
  -t neo4j-with-gds:5.26-2.13.9 .

# 推送到企业 registry（如有）
# docker tag neo4j-with-gds:5.26-2.13.9 registry.internal/neo4j-with-gds:5.26-2.13.9
# docker push registry.internal/neo4j-with-gds:5.26-2.13.9
```

修改 compose 文件中 Neo4j 服务的 image：

```yaml
neo4j-primary:
  image: neo4j-with-gds:5.26-2.13.9     # 原 neo4j:5.26-community
  environment:
    NEO4J_PLUGINS: '["apoc"]'           # 同方案 A
```

### 11.5 方案 C：业务不用 GDS → 整条移除

HA Agent 自身**不依赖 GDS**（CDC / SyncApplier / FailoverOrchestrator 全部用普通 Cypher）。如果业务也不跑图算法（中心性 / 社区检测 / 嵌入等），直接砍掉：

```yaml
NEO4J_PLUGINS: '["apoc"]'
NEO4J_dbms_security_procedures_unrestricted: "apoc.*"
NEO4J_dbms_security_procedures_allowlist: "apoc.*"
```

并把 plugins 目录里残留的 GDS jar 删除：

```bash
rm -f /opt/neo4j-{1,2}/plugins/neo4j-graph-data-science.jar
```

### 11.6 验证启动日志（三种方案通用）

修改后启动 Neo4j，**前 20 行**日志关键判定：

| 出现 | 含义 |
|---|---|
| `Installing Plugin 'apoc' from /var/lib/neo4j/labs/apoc-*-core.jar` | ✅ APOC 走本地复制，正常 |
| `Fetching versions.json ... from https://graphdatascience.ninja/` | ❌ GDS 仍在下载，方案没生效，检查 `NEO4J_PLUGINS` 是否真的去掉了 `graph-data-science` |
| `Plugin 'graph-data-science' loaded` 后续 Cypher 可调用 `gds.*` | ✅ jar 在 plugins 目录里被自动加载，方案生效 |

如要验证 GDS 确实加载成功，登入 cypher-shell：

```bash
docker exec -it neo4j-primary cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
  'CALL gds.version();'
```

输出形如 `"2.13.9"` 即说明 GDS 已就绪。

### 11.7 同样思路适用于其他插件

任何**未随官方镜像打包**的 Neo4j 插件（如 `neosemantics` / `apoc-extended` 部分版本）都会触发同样的"每次启动重新下载"行为。统一处理原则：

1. 一次性预下载 jar 到 `/opt/neo4j-*/plugins/`
2. 从 `NEO4J_PLUGINS` 列表移除该插件
3. 在 `NEO4J_dbms_security_procedures_unrestricted/allowlist` 里保留对应的命名空间（如 `n10s.*`）
4. Neo4j 启动自动扫描加载 plugins 目录里的所有 jar——**真正"放进去就能用"**
