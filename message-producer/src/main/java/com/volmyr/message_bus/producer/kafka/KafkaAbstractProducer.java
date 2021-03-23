package com.volmyr.message_bus.producer.kafka;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.collect.ImmutableMap;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.producer.KafkaResponse;
import com.volmyr.message_bus.producer.MessageProducer;
import com.volmyr.message_bus.producer.MessageProducerException;
import com.volmyr.message_bus.producer.MessageProducerResponse;
import com.volmyr.message_bus.producer.ResponseType;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of {@link MessageProducer} for Kafka platform.
 */
public abstract class KafkaAbstractProducer<K, V> implements MessageProducer {

  private static final Logger logger = LoggerFactory.getLogger(KafkaAbstractProducer.class);

  protected final String topic;
  protected final int timeoutMs;
  protected final Producer<K, V> producer;

  protected KafkaAbstractProducer(String topic, int timeoutMs, KafkaMessageProducerConfig config) {
    logger.debug("Instantiating producer {} for topic {}, with timeoutMs {} and config {}",
        this.getClass(), topic, timeoutMs, config);
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
    if (!isNullOrEmpty(config.getValueSerializer())) {
      propertiesBuilder
          .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getValueSerializer());
    }
    this.producer = new KafkaProducer<>(propertiesBuilder.build());
  }

  @Override
  public MessageProducerResponse send(MessageEvent event) throws MessageProducerException {
    logger.info("Try sending an event {}", event);
    try {
      RecordMetadata metadata = producer
          .send(getInstanceOfProducerRecord(event))
          .get(timeoutMs, TimeUnit.MILLISECONDS);
      logger.info("Got a producer response {}", metadata);
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
    } catch (InterruptedException e) {
      // Restore interrupted state...
      Thread.currentThread().interrupt();
      throw new MessageProducerException(e);
    } catch (ExecutionException | TimeoutException e) {
      throw new MessageProducerException(e);
    }
  }

  @Override
  public void close() {
    logger.debug("Closing producer {}", this.getClass());
    producer.close();
  }

  protected abstract ProducerRecord<K, V> getInstanceOfProducerRecord(MessageEvent event)
      throws MessageProducerException;
}
