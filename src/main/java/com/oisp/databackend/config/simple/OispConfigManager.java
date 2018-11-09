package com.oisp.databackend.config.simple;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oisp.databackend.exceptions.ConfigEnvironmentException;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Service
public final class OispConfigManager {
    public static final String OISP_BACKEND_TSDB_NAME_DUMMY = "dummy";

    private static final String OISP_BACKEND_CONFIG = "OISP_BACKEND_CONFIG";
    private static final String KAFKA_CONFIG = "kafka.config";
    private static final String TSDB_NAME = "tsdb.name";
    public static final String OISP_ZOOKEEPER_URI = "OISP_ZOOKEEPER_URI";
    private OispConfig simpleConfig;

    public OispConfigManager() {
    }

    @PostConstruct
    public void init() throws ConfigEnvironmentException {
        ObjectMapper objectMapper = new ObjectMapper();

        String rawBackendConfig = System.getenv(OISP_BACKEND_CONFIG);
        if (rawBackendConfig == null) {
            throw new ConfigEnvironmentException("Could not find environment variable " + OISP_BACKEND_CONFIG);
        }

        JsonNode backendNode = null;
        try {
            backendNode = objectMapper.readTree(rawBackendConfig);
        } catch (IOException e) {
            throw new ConfigEnvironmentException("Could not parse content of " + OISP_BACKEND_CONFIG, e);
        }

        String kafkaConfigVar = backendNode.get(KAFKA_CONFIG).asText();
        if (kafkaConfigVar == null) {
            throw new ConfigEnvironmentException("Could not find Kafka Config var in " + KAFKA_CONFIG + " field.");
        }

        String rawKafkaConfig = System.getenv(kafkaConfigVar);
        if (rawKafkaConfig == null) {
            throw new ConfigEnvironmentException("Could not find environment variable " + kafkaConfigVar);
        }

        JsonNode kafkaNode = null;
        try {
            kafkaNode = objectMapper.readTree(rawKafkaConfig);
        } catch (IOException e) {
            throw new ConfigEnvironmentException("Could not parse content of variable " + kafkaConfigVar, e);
        }

        // now setup simpleConfig object
        simpleConfig = new OispConfig();
        simpleConfig.setTsdbName(backendNode.get(TSDB_NAME).asText());

    }

    public OispConfig getSimpleConfig() {
        return simpleConfig;
    }
}

