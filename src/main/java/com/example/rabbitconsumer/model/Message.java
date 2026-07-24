package com.example.rabbitconsumer.model;

public class Message {
    private String mensagemId;
    private String message;

    public String getMensagemId() {
        return mensagemId;
    }

    public void setMensagemId(String mensagemId) {
        this.mensagemId = mensagemId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
