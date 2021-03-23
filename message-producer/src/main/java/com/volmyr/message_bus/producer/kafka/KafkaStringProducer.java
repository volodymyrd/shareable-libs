package com.volmyr.message_bus.producer.kafka;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.producer.MessageProducerException;
import java.io.IOException;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Implementation of {@link KafkaAbstractProducer} for <code>String</code> key and
 * <code>String</code> value.
 */
public final class KafkaStringProducer extends KafkaAbstractProducer<String, String> {

  public KafkaStringProducer(String topic, int timeoutMs, KafkaMessageProducerConfig config) {
    super(topic, timeoutMs, config);
  }

  @Override
  protected ProducerRecord<String, String> getInstanceOfProducerRecord(MessageEvent event)
      throws MessageProducerException {
    try {
      return new ProducerRecord<>(topic, toJson(event));
    } catch (IOException e) {
      throw new MessageProducerException(e);
    }
  }

  private static String toJson(Message message) throws IOException {
    return JsonFormat.printer()
        .usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder()
            .add(ImmutableList.of(Value.getDescriptor()))
            .build())
        .includingDefaultValueFields()
        .omittingInsignificantWhitespace()
        .print(message);
  }
}
