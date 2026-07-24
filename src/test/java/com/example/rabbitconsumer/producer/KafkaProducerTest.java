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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProducerTest {

    @Mock
    private KafkaTemplate<String, Message> kafkaTemplate;

    @Mock
    private CompletableFuture<SendResult<String, Message>> future;

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
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

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
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        kafkaProducer.enviarMensagem(testMessage);

        ArgumentCaptor<ProducerRecord<String, Message>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());

        ProducerRecord<String, Message> capturedRecord = recordCaptor.getValue();
        
        boolean hasTypeIdHeader = capturedRecord.headers().lastHeader("__TypeId__") != null;
        boolean hasSourceHeader = capturedRecord.headers().lastHeader("source") != null;
        
        assertEquals(true, hasTypeIdHeader);
        assertEquals(true, hasSourceHeader);
    }

    @Test
    void testEnviarMensagemWithNullMessage() {
        Message nullMessage = new Message();
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        kafkaProducer.enviarMensagem(nullMessage);

        ArgumentCaptor<ProducerRecord<String, Message>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());

        ProducerRecord<String, Message> capturedRecord = recordCaptor.getValue();
        assertEquals("jsonTopic", capturedRecord.topic());
    }
}
