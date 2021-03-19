package com.volmyr.message_bus.producer;

import com.volmyr.message_bus.MessageEvent;

/**
 * Interface for a message bus producer.
 */
public interface MessageProducer extends AutoCloseable {

  /**
   * Sends a {@link MessageEvent} event to the message bus and returns {@link
   * MessageProducerResponse}.
   */
  MessageProducerResponse send(MessageEvent event) throws MessageProducerException;
}
