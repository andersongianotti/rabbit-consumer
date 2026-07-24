package com.example.rabbitconsumer;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

class RabbitConsumerApplicationMainTest {

    @Test
    void mainDelegatesToSpringApplicationRun() {
        String[] args = {"--server.port=0"};

        try (MockedStatic<SpringApplication> springApplication = mockStatic(SpringApplication.class)) {
            ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
            springApplication
                    .when(() -> SpringApplication.run(RabbitConsumerApplication.class, args))
                    .thenReturn(context);

            RabbitConsumerApplication.main(args);

            springApplication.verify(() -> SpringApplication.run(RabbitConsumerApplication.class, args));
        }
    }

    @Test
    void mainPassesEmptyArgs() {
        String[] args = {};

        try (MockedStatic<SpringApplication> springApplication = mockStatic(SpringApplication.class)) {
            springApplication
                    .when(() -> SpringApplication.run(eq(RabbitConsumerApplication.class), any(String[].class)))
                    .thenReturn(mock(ConfigurableApplicationContext.class));

            RabbitConsumerApplication.main(args);

            springApplication.verify(() -> SpringApplication.run(RabbitConsumerApplication.class, args));
        }
    }
}
