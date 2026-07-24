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
import org.springframework.amqp.AmqpException;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        when(kafkaProducer.enviarMensagem(any(Message.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        rabbitMQConsumer.consumirMensagem(testMessage);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(kafkaProducer).enviarMensagem(messageCaptor.capture());

        Message capturedMessage = messageCaptor.getValue();
        assertEquals("123", capturedMessage.getMensagemId());
        assertEquals("Test message", capturedMessage.getMessage());
    }

    @Test
    void testConsumirMensagemWithEmptyMessage() {
        Message emptyMessage = new Message();
        emptyMessage.setMensagemId("");
        emptyMessage.setMessage("");
        when(kafkaProducer.enviarMensagem(any(Message.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        rabbitMQConsumer.consumirMensagem(emptyMessage);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(kafkaProducer).enviarMensagem(messageCaptor.capture());

        Message capturedMessage = messageCaptor.getValue();
        assertEquals("", capturedMessage.getMensagemId());
        assertEquals("", capturedMessage.getMessage());
    }

    @Test
    void testConsumirMensagemRejectsNull() {
        assertThrows(AmqpException.class, () -> rabbitMQConsumer.consumirMensagem(null));

        verify(kafkaProducer, never()).enviarMensagem(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testConsumirMensagemPropagatesInterruption() throws Exception {
        CompletableFuture<org.springframework.kafka.support.SendResult<String, Message>> future =
                org.mockito.Mockito.mock(CompletableFuture.class);
        when(future.get()).thenThrow(new InterruptedException("interrupted"));
        when(kafkaProducer.enviarMensagem(any(Message.class))).thenReturn(future);

        assertThrows(AmqpException.class, () -> rabbitMQConsumer.consumirMensagem(testMessage));
        org.junit.jupiter.api.Assertions.assertTrue(Thread.interrupted());
    }

    @Test
    void testConsumirMensagemPropagatesKafkaFailure() {
        CompletableFuture<org.springframework.kafka.support.SendResult<String, Message>> failed =
                new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafkaProducer.enviarMensagem(any(Message.class))).thenReturn(failed);

        AmqpException ex = assertThrows(AmqpException.class,
                () -> rabbitMQConsumer.consumirMensagem(testMessage));
        assertEquals("broker down", ex.getCause().getMessage());
    }
}
