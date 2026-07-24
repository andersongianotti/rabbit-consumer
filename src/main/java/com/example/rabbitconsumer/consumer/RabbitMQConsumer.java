package com.example.rabbitconsumer.consumer;

import com.example.rabbitconsumer.model.Message;
import com.example.rabbitconsumer.producer.KafkaProducer;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class RabbitMQConsumer {

    private final KafkaProducer kafkaProducer;

    public RabbitMQConsumer(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @RabbitListener(queues = "jsonQueue")
    public void consumirMensagem(Message mensagem) {
        System.out.println("Mensagem recebida do RabbitMQ:");
        System.out.println("ID: " + mensagem.getMensagemId());
        System.out.println("Conteúdo: " + mensagem.getMessage());
        
        kafkaProducer.enviarMensagem(mensagem);
    }
}
