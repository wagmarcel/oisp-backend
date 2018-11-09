package com.oisp.databackend.config.oisp;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Properties;

public class HBaseConfig {

    @JsonIgnore
    private Properties hadoopProperties;

    public void setHadoopProperties(Properties hadoopProperties) {
        this.hadoopProperties = hadoopProperties;
    }

    public Properties getHadoopProperties() {
        return hadoopProperties;
    }
}
