package com.volmyr.message_bus.producer.kafka;

import com.volmyr.message_bus.MessageEvent;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Implementation of {@link KafkaAbstractProducer} for <code>String</code> key and Google
 * <code>proto buffer</code> value as a <code>byte</code> array.
 */
public final class KafkaProtobufProducer extends KafkaAbstractProducer<String, byte[]> {

  public KafkaProtobufProducer(String topic, int timeoutMs, KafkaMessageProducerConfig config) {
    super(topic, timeoutMs, config);
  }

  @Override
  protected ProducerRecord<String, byte[]> getInstanceOfProducerRecord(MessageEvent event) {
    return new ProducerRecord<>(topic, event.toByteArray());
  }
}
