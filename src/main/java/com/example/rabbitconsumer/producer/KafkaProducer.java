package com.example.rabbitconsumer.producer;

import com.example.rabbitconsumer.model.Message;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    static final String TOPIC = "jsonTopic";

    private final KafkaTemplate<String, Message> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, Message> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, Message>> enviarMensagem(Message mensagem) {
        if (mensagem == null) {
            throw new IllegalArgumentException("Cannot publish a null message to Kafka");
        }

        String mensagemId = mensagem.getMensagemId();

        ProducerRecord<String, Message> record =
                new ProducerRecord<>(TOPIC, mensagem);

        record.headers().add(new RecordHeader("__TypeId__",
                Message.class.getName().getBytes(StandardCharsets.UTF_8)));

        record.headers().add(new RecordHeader("source",
                "rabbitmq".getBytes(StandardCharsets.UTF_8)));

        CompletableFuture<SendResult<String, Message>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish message {} to Kafka topic {}", mensagemId, TOPIC, ex);
            } else {
                log.info("Kafka published message {} to topic {}", mensagemId, TOPIC);
            }
        });

        return future;
    }
}
