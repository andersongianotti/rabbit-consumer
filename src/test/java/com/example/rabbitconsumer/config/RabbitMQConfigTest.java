package com.example.rabbitconsumer.config;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;

import static org.junit.jupiter.api.Assertions.*;

class RabbitMQConfigTest {

    private final RabbitMQConfig rabbitMQConfig = new RabbitMQConfig();

    @Test
    void testQueueBean() {
        Queue queue = rabbitMQConfig.jsonQueue();
        
        assertNotNull(queue);
        assertEquals("jsonQueue", queue.getName());
        assertTrue(queue.isDurable());
    }

    @Test
    void testExchangeBean() {
        TopicExchange exchange = rabbitMQConfig.exchange();
        
        assertNotNull(exchange);
        assertEquals("jsonExchange", exchange.getName());
    }

    @Test
    void testBindingBean() {
        Queue queue = rabbitMQConfig.jsonQueue();
        TopicExchange exchange = rabbitMQConfig.exchange();
        Binding binding = rabbitMQConfig.binding(queue, exchange);
        
        assertNotNull(binding);
        assertEquals("jsonQueue", binding.getDestination());
        assertEquals("json.routing.key", binding.getRoutingKey());
    }

    @Test
    void testMessageConverterBean() {
        MessageConverter converter = rabbitMQConfig.converter();
        
        assertNotNull(converter);
    }

    @Test
    void testTemplateBean() {
        ConnectionFactory connectionFactory = new CachingConnectionFactory("localhost", 5672);
        
        AmqpTemplate template = rabbitMQConfig.template(connectionFactory);
        
        assertNotNull(template);
        assertTrue(template instanceof RabbitTemplate);
    }
}
