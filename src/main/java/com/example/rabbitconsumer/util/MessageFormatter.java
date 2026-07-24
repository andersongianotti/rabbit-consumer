package com.example.rabbitconsumer.util;

import com.example.rabbitconsumer.model.Message;

public final class MessageFormatter {

    private MessageFormatter() {
    }

    public static String summary(Message message) {
        return message.getMensagemId() + " - " + message.getMessage();
    }
}
