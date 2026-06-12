# Build context: repository root (neo4j-HA/).
#   docker build -f docker/ha-agent/Dockerfile -t neo4j-ha-agent:1.0.0-SNAPSHOT .
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -pl src/ha-agent -am -DskipTests clean install \
  && mvn -pl src/ha-agent dependency:copy-dependencies -DincludeScope=runtime \
  && mkdir /out \
  && cp src/ha-agent/target/neo4j-ha-agent-*.jar /out/ha-agent.jar

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /out/ha-agent.jar /app/ha-agent.jar
COPY --from=build /build/src/ha-agent/target/dependency /app/lib
