package com.volmyr.message_bus.consumer;

import com.volmyr.message_bus.MessageEvent;

/**
 * Interface for a message bus producer.
 */
public interface MessageConsumer extends Runnable {

  /**
   * Handles of {@link MessageEvent}.
   */
  void handle(MessageEvent event) throws MessageConsumerException;

  /**
   * Closes the consumer and releases resources.
   */
  void shutdown();
}
