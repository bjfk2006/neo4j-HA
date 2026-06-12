package com.neo4j.ha.common.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);
    private static final Pattern ENV_VAR_PATTERN = Pattern.compile("\\$\\{([^:}]+)(?::-(.*?))?}");
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    public static HaConfig load(String configPath) throws IOException {
        log.info("Loading configuration from: {}", configPath);
        String content = Files.readString(Path.of(configPath));
        String resolved = resolveEnvVars(content);
        return YAML_MAPPER.readValue(resolved, HaConfig.class);
    }

    public static HaConfig loadFromClasspath(String resourceName) throws IOException {
        try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (is == null) {
                throw new IOException("Resource not found: " + resourceName);
            }
            String content = new String(is.readAllBytes());
            String resolved = resolveEnvVars(content);
            return YAML_MAPPER.readValue(resolved, HaConfig.class);
        }
    }

    static String resolveEnvVars(String content) {
        Matcher matcher = ENV_VAR_PATTERN.matcher(content);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String envName = matcher.group(1);
            String defaultValue = matcher.group(2);
            String envValue = System.getenv(envName);
            String replacement;
            if (envValue != null) {
                replacement = envValue;
            } else if (defaultValue != null) {
                replacement = defaultValue;
            } else {
                replacement = "";
                log.warn("Environment variable '{}' not set and no default provided", envName);
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
