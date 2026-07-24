package com.example.rabbitconsumer.producer;

import com.example.rabbitconsumer.model.Message;
import com.example.rabbitconsumer.util.KafkaHeaderUtils;
import com.example.rabbitconsumer.util.MessageFormatter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    public static final String TOPIC = "jsonTopic";
    public static final String TYPE_ID_HEADER = "__TypeId__";
    public static final String SOURCE_HEADER = "source";
    public static final String SOURCE = "rabbitmq";

    private final KafkaTemplate<String, Message> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, Message> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void enviarMensagem(Message mensagem) {
        ProducerRecord<String, Message> record =
                new ProducerRecord<>(TOPIC, mensagem);

        KafkaHeaderUtils.addStringHeader(record, TYPE_ID_HEADER, Message.class.getName());
        KafkaHeaderUtils.addStringHeader(record, SOURCE_HEADER, SOURCE);

        kafkaTemplate.send(record);

        System.out.println("Kafka publicou: " + MessageFormatter.summary(mensagem));
    }
}
