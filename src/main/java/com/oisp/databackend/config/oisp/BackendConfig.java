package com.oisp.databackend.config.oisp;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BackendConfig {

    private String tsdbName;
    private String kafkaConfigLink;
    private String zookeeperConfigLink;
    private String kerberosConfigLink;

    @JsonIgnore
    private KafkaConfig kafkaConfig;
    @JsonIgnore
    private ZookeeperConfig zookeeperConfig;
    @JsonIgnore
    private KerberosConfig kerberosConfig;

    public String getTsdbName() {
        return tsdbName;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public ZookeeperConfig getZookeeperConfig() {
        return zookeeperConfig;
    }

    public String getKafkaConfigLink() {
        return kafkaConfigLink;
    }

    public String getKerberosConfigLink() {
        return kerberosConfigLink;
    }

    public String getZookeeperConfigLink() {
        return zookeeperConfigLink;
    }

    public void setTsdbName(String tsdbName) {
        this.tsdbName = tsdbName;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public void setZookeeperConfig(ZookeeperConfig zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }

    public void setKerberosConfig(KerberosConfig kerberosConfig) {
        this.kerberosConfig = kerberosConfig;
    }

    public void setKafkaConfigLink(String kafkaConfigLink) {
        this.kafkaConfigLink = kafkaConfigLink;
    }

    public void setKerberosConfigLink(String kerberosConfigLink) {
        this.kerberosConfigLink = kerberosConfigLink;
    }

    public void setZookeeperConfigLink(String zookeeperConfigLink) {
        this.zookeeperConfigLink = zookeeperConfigLink;
    }
}
