package com.volmyr.message_bus.consumer.kafka;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.consumer.MessageConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Abstract implementation of {@link MessageConsumer} for Kafka platform for <code>String</code> key
 * and Google <code>proto buffer</code> value as a <code>byte</code> array.
 */
public abstract class KafkaProtobufConsumer extends KafkaAbstractConsumer<String, byte[]> {

  protected KafkaProtobufConsumer(
      ImmutableList<String> topics, int pollDurationMs, KafkaMessageConsumerConfig config) {
    super(topics, pollDurationMs, config);
  }

  @Override
  protected MessageEvent convert(ConsumerRecord<String, byte[]> record)
      throws KafkaConsumerRecordConvertException {
    try {
      return MessageEvent.parseFrom(record.value());
    } catch (InvalidProtocolBufferException e) {
      throw new KafkaConsumerRecordConvertException(e);
    }
  }
}
