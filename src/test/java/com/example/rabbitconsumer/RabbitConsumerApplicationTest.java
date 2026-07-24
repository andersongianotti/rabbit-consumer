package com.example.rabbitconsumer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@TestPropertySource(properties = {
    "spring.rabbitmq.host=localhost",
    "spring.rabbitmq.port=5672",
    "spring.rabbitmq.username=guest",
    "spring.rabbitmq.password=guest",
    "spring.kafka.bootstrap-servers=localhost:9092"
})
class RabbitConsumerApplicationTest {

    @Test
    void contextLoads() {
        assertNotNull(RabbitConsumerApplication.class);
    }
}
