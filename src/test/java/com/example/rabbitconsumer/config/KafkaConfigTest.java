package com.example.rabbitconsumer.config;

import com.example.rabbitconsumer.model.Message;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import static org.junit.jupiter.api.Assertions.*;

class KafkaConfigTest {

    private final KafkaConfig kafkaConfig = new KafkaConfig();

    @Test
    void testProducerFactoryBean() {
        ProducerFactory<String, Message> producerFactory = kafkaConfig.producerFactory();
        
        assertNotNull(producerFactory);
        assertTrue(producerFactory instanceof org.springframework.kafka.core.DefaultKafkaProducerFactory);
    }

    @Test
    void testKafkaTemplateBean() {
        KafkaTemplate<String, Message> kafkaTemplate = kafkaConfig.kafkaTemplate();
        
        assertNotNull(kafkaTemplate);
        assertNotNull(kafkaTemplate.getProducerFactory());
    }

    @Test
    void testProducerFactoryConfiguration() {
        ProducerFactory<String, Message> producerFactory = kafkaConfig.producerFactory();
        
        assertNotNull(producerFactory);
        assertNotNull(producerFactory.getConfigurationProperties());
        
        java.util.Map<String, Object> config = producerFactory.getConfigurationProperties();
        assertEquals("localhost:9092", config.get(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }
}
