package com.oisp.databackend.config.simple;

public class BackendConfig {

    private String tsdbName;
    private String kafkaConfig;
    private String zookeeperConfig;

    public String getTsdbName() {
        return tsdbName;
    }

    public String getKafkaConfig() {
        return kafkaConfig;
    }

    public String getZookeeperConfig() {
        return zookeeperConfig;
    }

    public void setTsdbName(String tsdbName) {
        this.tsdbName = tsdbName;
    }

    public void setKafkaConfig(String kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public void setZookeeperConfig(String zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }
}
