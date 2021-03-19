package com.volmyr.message_bus.producer;

/**
 * Exception throws by {@link MessageProducer}.
 */
public final class MessageProducerException extends Exception {
    public MessageProducerException(Throwable cause) {
        super(cause);
    }
}
