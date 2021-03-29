package com.volmyr.message_bus.consumer;

/**
 * Exception throws by {@link MessageConsumer}.
 */
public final class MessageConsumerException extends Exception {
    public MessageConsumerException(Throwable cause) {
        super(cause);
    }

    public MessageConsumerException(String message) {
        super(message);
    }
}
