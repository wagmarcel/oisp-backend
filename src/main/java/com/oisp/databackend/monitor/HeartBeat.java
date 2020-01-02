package com.oisp.databackend.monitor;

import com.oisp.databackend.config.oisp.OispConfig;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import javax.annotation.PreDestroy;


@Component
public class HeartBeat implements ApplicationListener<ApplicationReadyEvent>, Runnable { 

    private static final Logger logger = LoggerFactory.getLogger(HeartBeat.class);

    private KafkaProducer<String, String> kafkaProducer;

    private String topic;    

    @Autowired
    private OispConfig oispConfig;


    @Autowired
    public HeartBeat(KafkaProducer<String, String> kafkaProducer) {
        logger.info("================= HeartBeat");
        this.kafkaProducer = kafkaProducer;
    }


    @Override
    public void onApplicationEvent(final ApplicationReadyEvent event) {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setThreadNamePrefix("HeatBeat");
        s.initialize();
        logger.info("================= Ready1");
        topic = oispConfig.getBackendConfig().getKafkaConfig().getTopicsHeartbeatName();

        logger.info("================= Ready2");

        Integer period = oispConfig.getBackendConfig().getKafkaConfig().getTopicsHeartbeatInterval();
        s.scheduleAtFixedRate(this, period);
    }
 
    @Override
    public void run() {
        if (kafkaProducer != null) {
            ProducerRecord<String, String> message = new ProducerRecord<String, String>(topic, "backend");
            kafkaProducer.send(message);
        }
    }

    @PreDestroy
    protected void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

} 