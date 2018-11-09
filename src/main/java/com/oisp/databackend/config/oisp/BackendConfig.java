package com.oisp.databackend.config.oisp;


import com.fasterxml.jackson.annotation.JsonIgnore;

public class BackendConfig {

    private String tsdbName;
    private String hbaseTablePrefix;

    @JsonIgnore
    private KafkaConfig kafkaConfig;
    @JsonIgnore
    private ZookeeperConfig zookeeperConfig;
    @JsonIgnore
    private KerberosConfig kerberosConfig;
    @JsonIgnore
    private HBaseConfig hbaseConfig;

    public String getTsdbName() {
        return tsdbName;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public ZookeeperConfig getZookeeperConfig() {
        return zookeeperConfig;
    }

    public KerberosConfig getKerberosConfig() {
        return kerberosConfig;
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

    public String getHbaseTablePrefix() {
        return hbaseTablePrefix;
    }

    public void setHbaseTablePrefix(String hbaseTablePrefix) {
        this.hbaseTablePrefix = hbaseTablePrefix;
    }

    public HBaseConfig getHbaseConfig() {
        return hbaseConfig;
    }

    public void setHbaseConfig(HBaseConfig hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }
}
