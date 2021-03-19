package com.volmyr.message_bus.producer;

/**
 * Interface for a message bus producer.
 */
public interface MessageProducer extends AutoCloseable {

  /**
   * Sends a {@link MessageProducerRequest} message to the message bus and returns {@link
   * MessageProducerResponse}.
   */
  MessageProducerResponse send(MessageProducerRequest request) throws MessageProducerException;
}
