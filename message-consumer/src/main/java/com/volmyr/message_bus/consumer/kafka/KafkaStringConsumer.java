package com.volmyr.message_bus.consumer.kafka;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.consumer.MessageConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Abstract implementation of {@link MessageConsumer} for Kafka platform for <code>String</code> key
 * and <code>String</code> value.
 */
public abstract class KafkaStringConsumer extends KafkaAbstractConsumer<String, String> {

  protected KafkaStringConsumer(
      ImmutableList<String> topics, int pollDurationMs, KafkaMessageConsumerConfig config) {
    super(topics, pollDurationMs, config);
  }

  @Override
  protected MessageEvent convert(ConsumerRecord<String, String> record)
      throws KafkaConsumerRecordConvertException {
    try {
      return (MessageEvent) fromJson(record.value(), MessageEvent.newBuilder());
    } catch (InvalidProtocolBufferException e) {
      throw new KafkaConsumerRecordConvertException(e);
    }
  }

  private static Message fromJson(String json, Message.Builder builder)
      throws InvalidProtocolBufferException {
    JsonFormat
        .parser()
        .usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder()
            .add(ImmutableList.of(Value.getDescriptor()))
            .build())
        .merge(json, builder);
    return builder.build();
  }
}
