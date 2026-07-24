package com.example.rabbitconsumer.consumer;

import com.example.rabbitconsumer.model.Message;
import com.example.rabbitconsumer.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class RabbitMQConsumer {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQConsumer.class);

    private final KafkaProducer kafkaProducer;

    public RabbitMQConsumer(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @RabbitListener(queues = "jsonQueue")
    public void consumirMensagem(Message mensagem) {
        if (mensagem == null) {
            throw new AmqpException("Received a null message from RabbitMQ");
        }

        log.info("Message received from RabbitMQ: id={}", mensagem.getMensagemId());

        try {
            kafkaProducer.enviarMensagem(mensagem).get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AmqpException(
                    "Interrupted while forwarding message " + mensagem.getMensagemId() + " to Kafka", ex);
        } catch (ExecutionException ex) {
            log.error("Failed to forward message {} to Kafka", mensagem.getMensagemId(), ex.getCause());
            throw new AmqpException(
                    "Failed to forward message " + mensagem.getMensagemId() + " to Kafka", ex.getCause());
        }
    }
}
