package com.oisp.databackend.handlers.kafkaconsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oisp.databackend.config.oisp.OispConfig;
import com.oisp.databackend.datasources.DataDao;
import com.oisp.databackend.datastructures.Observation;
import com.oisp.databackend.exceptions.ServiceException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;


@EnableKafka
@Configuration
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final int MAX_ATTEMPTS = 3;
    private static final int MAX_FAILURES = 3;
    @Autowired
    private KafkaConsumerProperties kafkaConsumerProperties;
    @Autowired
    private OispConfig oispConfig;
    @Autowired
    //@Qualifier("dataDaoImpl")
    private DataDao dataDao;

    public KafkaConsumerProperties getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }

    public void setKafkaConsumerProperties(KafkaConsumerProperties kafkaConsumerProperties) {
        this.kafkaConsumerProperties = kafkaConsumerProperties;
    }

    public OispConfig getOispConfig() {
        return oispConfig;
    }

    public void setOispConfig(OispConfig oispConfig) {
        this.oispConfig = oispConfig;
    }

    public DataDao getDataDao() {
        return dataDao;
    }

    public void setDataDao(DataDao dataDao) {
        this.dataDao = dataDao;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                oispConfig.getBackendConfig().getKafkaConfig().getUri());
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "backendConsumerGroup");
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                oispConfig.getBackendConfig().getKafkaConfig().getMaxPayloadSize());
        props.put(
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                oispConfig.getBackendConfig().getKafkaConfig().getMaxPayloadSize());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory());
        factory.setStatefulRetry(true);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setBackOffPolicy(new ExponentialRandomBackOffPolicy());
        retryTemplate.setThrowLastExceptionOnExhausted(true);
        retryTemplate.setRetryPolicy(new SimpleRetryPolicy(MAX_ATTEMPTS));
        factory.setRetryTemplate(retryTemplate);
        return factory;
    }


    @Bean
    public ErrorHandler seekToCurrentErrorHandler() {
        SeekToCurrentErrorHandler seekToCurrentErrorHandler = new SeekToCurrentErrorHandler(MAX_FAILURES);
        seekToCurrentErrorHandler.setCommitRecovered(true);
        return seekToCurrentErrorHandler;
    }

    @KafkaListener(topics = "#{kafkaConsumerProperties.getTopic()}")
    public void receive(String rawObservations) throws IOException, ServiceException {
        ObjectMapper mapper = new ObjectMapper();
        Observation[] observations = mapper.readValue(rawObservations, Observation[].class);
        logger.info("Received Observations in topic " + kafkaConsumerProperties.getTopic()
                + ". Message: " + observations.toString());
        if (!dataDao.put(observations)) {
            throw new ServiceException("Data store error.");
        }

    }
}