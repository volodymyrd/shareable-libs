package com.volmyr.message_bus.producer.kafka;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.volmyr.message_bus.producer.KafkaResponse;
import com.volmyr.message_bus.producer.MessageProducer;
import com.volmyr.message_bus.producer.MessageProducerException;
import com.volmyr.message_bus.producer.MessageProducerRequest;
import com.volmyr.message_bus.producer.MessageProducerResponse;
import com.volmyr.message_bus.producer.ResponseType;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Implementation of {@link MessageProducer} for Kafka platform.
 */
public final class KafkaStringProducer implements MessageProducer {

  private final String topic;
  private final int timeoutMs;
  private final Producer<String, String> producer;

  public KafkaStringProducer(String topic, int timeoutMs, KafkaMessageProducerConfig config) {
    this.topic = topic;
    this.timeoutMs = timeoutMs;
    ImmutableMap.Builder<String, Object> propertiesBuilder = ImmutableMap.builder();
    propertiesBuilder.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    propertiesBuilder.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
    propertiesBuilder.put(ProducerConfig.RETRIES_CONFIG, config.getRetries());
    if (config.getLingerMs() > 0) {
      propertiesBuilder.put(ProducerConfig.LINGER_MS_CONFIG, config.getLingerMs());
    }
    if (!isNullOrEmpty(config.getKeySerializer())) {
      propertiesBuilder.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getKeySerializer());
    }
    if (!isNullOrEmpty(config.getKeySerializer())) {
      propertiesBuilder
          .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getValueSerializer());
    }
    this.producer = new KafkaProducer<>(propertiesBuilder.build());
  }

  @Override
  public MessageProducerResponse send(MessageProducerRequest request)
      throws MessageProducerException {
    try {
      RecordMetadata metadata = producer
          .send(new ProducerRecord<>(topic, toJson(request)))
          .get(timeoutMs, TimeUnit.MILLISECONDS);
      return MessageProducerResponse.newBuilder()
          .setType(ResponseType.KAFKA)
          .setKafkaResponse(KafkaResponse.newBuilder()
              .setTimestamp(metadata.timestamp())
              .setOffset(metadata.offset())
              .setSerializedKeySize(metadata.serializedKeySize())
              .setSerializedValueSize(metadata.serializedValueSize())
              .setTopic(metadata.topic())
              .setPartition(metadata.partition())
              .build())
          .build();
    } catch (Exception e) {
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

  @Override
  public void close() throws Exception {
    producer.close();
  }
}
