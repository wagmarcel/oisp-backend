package com.oisp.databackend.config.oisp;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Properties;

@ConfigurationProperties(prefix = "backendConfig")
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackendConfig {

    private String tsdbName;
    private String objectStoreName;
    private String hbaseTablePrefix;

    @JsonIgnore
    private KafkaConfig kafkaConfig;
    @JsonIgnore
    private ZookeeperConfig zookeeperConfig;
    @JsonIgnore
    private KerberosConfig kerberosConfig;
    @JsonIgnore
    private HBaseConfig hbaseConfig;
    @JsonIgnore
    private Properties tsdbProperties;
    @JsonIgnore
    private JaegerConfig jaegerConfig;

    @JsonIgnore
    private Properties objectStoreProperties;

    public JaegerConfig getJaegerConfig() {
        return jaegerConfig;
    }

    public void setJaegerConfig(JaegerConfig jaegerConfig) {
        this.jaegerConfig = jaegerConfig;
    }

    public Properties getTsdbProperties() {
        return tsdbProperties;
    }

    public void setTsdbProperties(Properties tsdbProperties) {
        this.tsdbProperties = tsdbProperties;
    }

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
    public Properties getObjectStoreProperties() {
        return objectStoreProperties;
    }

    public void setObjectStoreProperties(Properties objectStoreProperties) {
        this.objectStoreProperties = objectStoreProperties;
    }

    public String getObjectStoreName() {
        return objectStoreName;
    }

    public void setObjectStoreName(String objectStoreName) {
        this.objectStoreName = objectStoreName;
    }
}
