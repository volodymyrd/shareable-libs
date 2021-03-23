package com.volmyr.message_bus.consumer.kafka;

import com.volmyr.message_bus.MessageEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The exception throws when converting {@link ConsumerRecord} to {@link MessageEvent} fails.
 */
public final class KafkaConsumerRecordConvertException extends Exception {

  public KafkaConsumerRecordConvertException(Throwable cause) {
    super(cause);
  }
}
