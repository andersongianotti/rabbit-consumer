package com.example.rabbitconsumer.util;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;

public final class KafkaHeaderUtils {

    private KafkaHeaderUtils() {
    }

    public static void addStringHeader(ProducerRecord<?, ?> record, String key, String value) {
        record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }
}
