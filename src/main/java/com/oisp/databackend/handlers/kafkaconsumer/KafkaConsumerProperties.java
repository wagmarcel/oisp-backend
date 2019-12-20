package com.oisp.databackend.handlers.kafkaconsumer;

import com.oisp.databackend.config.oisp.OispConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@ConfigurationProperties(prefix = "kafka.consumer")
public class KafkaConsumerProperties {
    @Autowired
    private OispConfig oispConfig;
    private String topic;

    public OispConfig getOispConfig() {
        return oispConfig;
    }

    public void setOispConfig(OispConfig oispConfig) {
        this.oispConfig = oispConfig;
    }

    @PostConstruct
    void initialize() {
        topic = oispConfig.getBackendConfig().getKafkaConfig().getTopicsObservations();
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }



}