package com.oisp.databackend.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.IOException;

@Service
public class KafkaConsumerService {
    private final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Value("${backendConfig.tsdbName}")
    String topic;

    @KafkaListener(topics = "${backendConfig.kafkaConfig.topicsObservation}",
            groupId = "backendMetricsConsumer",
            bootstrapServers = "${backendConfig.kafkaConfig.topicsObservation}")
    public void consume(String message) throws IOException {
        logger.info(String.format("#### -> Consumed message -> %s", message));
    }
}
