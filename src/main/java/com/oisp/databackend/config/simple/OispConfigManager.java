package com.oisp.databackend.config.simple;


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

        BackendConfig backendConfig = null;
        try {
            backendConfig = objectMapper.readValue(rawBackendConfig, BackendConfig.class);
        } catch (IOException e) {
            throw new ConfigEnvironmentException("Could not parse content of " + OISP_BACKEND_CONFIG, e);
        }

        String rawKafkaConfig = System.getenv(backendConfig.getKafkaConfig());
        if (rawKafkaConfig == null) {
            throw new ConfigEnvironmentException("Could not find Kafka configuration in " + rawBackendConfig );
        }

        KafkaConfig kafkaConfig = null;
        try {
            kafkaConfig = objectMapper.readValue(rawKafkaConfig, KafkaConfig.class);
        } catch (IOException e) {
            throw new ConfigEnvironmentException("Could not parse content of " + rawKafkaConfig, e);
        }

        String rawZookeeperConfig = System.getenv(backendConfig.getZookeeperConfig());
        if (rawZookeeperConfig == null) {
            throw new ConfigEnvironmentException("Could not find Zookeeper configuration in " + rawBackendConfig );
        }

        ZookeeperConfig zookeeperConfig = null;
        try {
            zookeeperConfig = objectMapper.readValue(rawZookeeperConfig, ZookeeperConfig.class);
        } catch (IOException e) {
            throw new ConfigEnvironmentException("Could not parse content of " + rawZookeeperConfig, e);
        }



        // now setup simpleConfig object
        simpleConfig = new OispConfig();
        simpleConfig.setBackendConfig(backendConfig);
        simpleConfig.setKafkaConfig(kafkaConfig);
        simpleConfig.setZookeeperConfig(zookeeperConfig);
    }

    public OispConfig getSimpleConfig() {
        return simpleConfig;
    }
}

