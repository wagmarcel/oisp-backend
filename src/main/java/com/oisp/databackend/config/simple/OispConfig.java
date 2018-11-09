package com.oisp.databackend.config.simple;

public class OispConfig {
    BackendConfig backendConfig;
    KafkaConfig kafkaConfig;
    ZookeeperConfig zookeeperConfig;

    public BackendConfig getBackendConfig() {
        return backendConfig;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public ZookeeperConfig getZookeeperConfig() {
        return zookeeperConfig;
    }

    public void setBackendConfig(BackendConfig backendConfig) {
        this.backendConfig = backendConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public void setZookeeperConfig(ZookeeperConfig zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }
}
