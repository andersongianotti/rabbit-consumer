package com.example.rabbitconsumer.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MessageTest {

    private Message message;

    @BeforeEach
    void setUp() {
        message = new Message();
    }

    @Test
    void testGettersAndSetters() {
        message.setMensagemId("123");
        message.setMessage("Test message");

        assertEquals("123", message.getMensagemId());
        assertEquals("Test message", message.getMessage());
    }

    @Test
    void testSetMensagemId() {
        message.setMensagemId("456");
        assertEquals("456", message.getMensagemId());
    }

    @Test
    void testSetMessage() {
        message.setMessage("Another message");
        assertEquals("Another message", message.getMessage());
    }

    @Test
    void testEmptyValues() {
        message.setMensagemId("");
        message.setMessage("");

        assertEquals("", message.getMensagemId());
        assertEquals("", message.getMessage());
    }

    @Test
    void testNullValues() {
        assertNull(message.getMensagemId());
        assertNull(message.getMessage());
    }

    @Test
    void testLongMensagemId() {
        String longId = "a".repeat(100);
        message.setMensagemId(longId);
        assertEquals(longId, message.getMensagemId());
    }

    @Test
    void testLongMessage() {
        String longMessage = "b".repeat(500);
        message.setMessage(longMessage);
        assertEquals(longMessage, message.getMessage());
    }
}
