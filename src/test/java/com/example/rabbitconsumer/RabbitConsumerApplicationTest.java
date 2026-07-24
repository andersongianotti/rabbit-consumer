package com.example.rabbitconsumer;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

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

    @Test
    void mainDelegatesToSpringApplication() {
        String[] args = {"--server.port=0"};
        try (MockedStatic<SpringApplication> mocked = mockStatic(SpringApplication.class)) {
            RabbitConsumerApplication.main(args);
            mocked.verify(() -> SpringApplication.run(RabbitConsumerApplication.class, args), times(1));
        }
    }
}
