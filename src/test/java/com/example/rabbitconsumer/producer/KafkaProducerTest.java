package com.example.rabbitconsumer.producer;

import com.example.rabbitconsumer.model.Message;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProducerTest {

    @Mock
    private KafkaTemplate<String, Message> kafkaTemplate;

    @InjectMocks
    private KafkaProducer kafkaProducer;

    private Message testMessage;

    @BeforeEach
    void setUp() {
        testMessage = new Message();
        testMessage.setMensagemId("123");
        testMessage.setMessage("Test message");
    }

    @Test
    void testEnviarMensagem() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        kafkaProducer.enviarMensagem(testMessage);

        ArgumentCaptor<ProducerRecord<String, Message>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());

        ProducerRecord<String, Message> capturedRecord = recordCaptor.getValue();
        assertEquals("jsonTopic", capturedRecord.topic());
        assertEquals("123", capturedRecord.value().getMensagemId());
        assertEquals("Test message", capturedRecord.value().getMessage());
    }

    @Test
    void testEnviarMensagemWithHeaders() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        kafkaProducer.enviarMensagem(testMessage);

        ArgumentCaptor<ProducerRecord<String, Message>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());

        ProducerRecord<String, Message> capturedRecord = recordCaptor.getValue();

        boolean hasTypeIdHeader = capturedRecord.headers().headers("__TypeId__").iterator().hasNext();
        boolean hasSourceHeader = capturedRecord.headers().headers("source").iterator().hasNext();

        assertTrue(hasTypeIdHeader);
        assertTrue(hasSourceHeader);
    }

    @Test
    void testEnviarMensagemWithNullMessage() {
        assertThrows(IllegalArgumentException.class,
                () -> kafkaProducer.enviarMensagem(null));

        verify(kafkaTemplate, org.mockito.Mockito.never()).send(any(ProducerRecord.class));
    }

    @Test
    void testEnviarMensagemReturnsTemplateFuture() {
        CompletableFuture<SendResult<String, Message>> expected = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(expected);

        CompletableFuture<SendResult<String, Message>> actual = kafkaProducer.enviarMensagem(testMessage);

        assertSame(expected, actual);
    }

    @Test
    void testEnviarMensagemPropagatesSendFailure() {
        CompletableFuture<SendResult<String, Message>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        CompletableFuture<SendResult<String, Message>> result = kafkaProducer.enviarMensagem(testMessage);

        assertTrue(result.isCompletedExceptionally());
        assertThrows(ExecutionException.class, result::get);
    }
}
