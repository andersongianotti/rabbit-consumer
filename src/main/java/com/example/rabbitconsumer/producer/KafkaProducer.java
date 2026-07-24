package com.example.rabbitconsumer.producer;

import com.example.rabbitconsumer.model.Message;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class KafkaProducer {

    private final KafkaTemplate<String, Message> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, Message> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void enviarMensagem(Message mensagem) {
        ProducerRecord<String, Message> record =
                new ProducerRecord<>("jsonTopic", mensagem);

        record.headers().add(new RecordHeader("__TypeId__",
                Message.class.getName().getBytes(StandardCharsets.UTF_8)));

        record.headers().add(new RecordHeader("source",
                "rabbitmq".getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record);

        System.out.println("Kafka publicou: " + mensagem.getMensagemId() + " - " + mensagem.getMessage());
    }
}
