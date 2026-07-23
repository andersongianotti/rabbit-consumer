package com.example.rabbitconsumer.consumer;

import com.example.rabbitconsumer.model.Message;
import com.example.rabbitconsumer.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RabbitMQConsumerTest {

    @Mock
    private KafkaProducer kafkaProducer;

    @InjectMocks
    private RabbitMQConsumer rabbitMQConsumer;

    private Message testMessage;

    @BeforeEach
    void setUp() {
        testMessage = new Message();
        testMessage.setMensagemId("123");
        testMessage.setMessage("Test message");
    }

    @Test
    void testConsumirMensagem() {
        rabbitMQConsumer.consumirMensagem(testMessage);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(kafkaProducer).enviarMensagem(messageCaptor.capture());

        Message capturedMessage = messageCaptor.getValue();
        assertEquals("123", capturedMessage.getMensagemId());
        assertEquals("Test message", capturedMessage.getMessage());
    }

    @Test
    void testConsumirMensagemWithNullFields() {
        Message nullMessage = new Message();
        rabbitMQConsumer.consumirMensagem(nullMessage);

        verify(kafkaProducer).enviarMensagem(nullMessage);
    }

    @Test
    void testConsumirMensagemWithEmptyMessage() {
        Message emptyMessage = new Message();
        emptyMessage.setMensagemId("");
        emptyMessage.setMessage("");
        
        rabbitMQConsumer.consumirMensagem(emptyMessage);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(kafkaProducer).enviarMensagem(messageCaptor.capture());

        Message capturedMessage = messageCaptor.getValue();
        assertEquals("", capturedMessage.getMensagemId());
        assertEquals("", capturedMessage.getMessage());
    }
}
