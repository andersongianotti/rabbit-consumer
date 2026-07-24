package com.example.rabbitconsumer.consumer;

import com.example.rabbitconsumer.config.RabbitMQConfig;
import com.example.rabbitconsumer.model.Message;
import com.example.rabbitconsumer.producer.KafkaProducer;
import com.example.rabbitconsumer.util.MessageFormatter;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class RabbitMQConsumer {

    private final KafkaProducer kafkaProducer;

    public RabbitMQConsumer(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @RabbitListener(queues = RabbitMQConfig.QUEUE)
    public void consumirMensagem(Message mensagem) {
        System.out.println("Mensagem recebida do RabbitMQ: " + MessageFormatter.summary(mensagem));

        kafkaProducer.enviarMensagem(mensagem);
    }
}
