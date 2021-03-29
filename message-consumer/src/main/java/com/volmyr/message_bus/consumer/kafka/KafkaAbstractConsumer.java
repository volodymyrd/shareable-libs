package com.volmyr.message_bus.consumer.kafka;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.consumer.MessageConsumer;
import com.volmyr.message_bus.consumer.MessageConsumerException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of {@link MessageConsumer} for Kafka platform.
 */
public abstract class KafkaAbstractConsumer<K, V> implements MessageConsumer {

  private static final Logger logger = LoggerFactory.getLogger(KafkaAbstractConsumer.class);

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ImmutableList<String> topics;
  private final int pollDurationMs;
  private final boolean enableAutoCommit;
  private final KafkaConsumer<K, V> consumer;

  protected KafkaAbstractConsumer(
      ImmutableList<String> topics,
      int pollDurationMs,
      KafkaMessageConsumerConfig config) {
    logger.debug("Instantiating consumer {} for topics {}, with pollDurationMs {} and config {}",
        this.getClass(), topics, pollDurationMs, config);
    this.topics = topics;
    this.pollDurationMs = pollDurationMs;
    this.enableAutoCommit = config.getEnableAutoCommit();
    ImmutableMap.Builder<String, Object> propertiesBuilder = ImmutableMap.builder();
    propertiesBuilder.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    propertiesBuilder.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
    propertiesBuilder.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getEnableAutoCommit());
    if (enableAutoCommit) {
      propertiesBuilder.put(
          ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getAutoCommitIntervalMs());
    }
    propertiesBuilder.put(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getAutoOffsetReset().name().toLowerCase());

    if (!isNullOrEmpty(config.getKeyDeserializer())) {
      propertiesBuilder
          .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
    }
    if (!isNullOrEmpty(config.getValueDeserializer())) {
      propertiesBuilder
          .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getValueDeserializer());
    }
    this.consumer = new KafkaConsumer<>(propertiesBuilder.build());
  }

  @Override
  public void run() {
    logger.debug("Start a consumer {}", this);
    try {
      consumer.subscribe(topics);
      while (!closed.get()) {
        ConsumerRecords<K, V> records = consumer.poll(pollDurationMs);
        if (!records.isEmpty()) {
          logger.info("Consumer {} got {} records, start handling...", this, records.count());
        } else {
          logger.debug("Consumer {} got no records", this);
        }
        for (ConsumerRecord<K, V> record : records) {
          logger.info("Start handling a record {}...", record);
          handle(record);
        }
      }
    } catch (WakeupException e) {
      // Ignore exception if closing
      if (!closed.get()) {
        throw e;
      }
    } finally {
      logger.debug("Close the consumer {}", this);
      consumer.close();
    }
  }

  @Override
  public void shutdown() {
    closed.set(true);
    consumer.wakeup();
  }

  protected abstract MessageEvent convert(ConsumerRecord<K, V> record)
      throws KafkaConsumerRecordConvertException;

  private void handle(ConsumerRecord<K, V> record) {
    try {
      handle(convert(record));
      commit(record);
    } catch (MessageConsumerException e) {
      logger.error("Error handling a record " + record, e);
      logger.debug("Overriding the fetch offsets for the next pull, because of error...");
      consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
    } catch (KafkaConsumerRecordConvertException e) {
      logger.error("Error handling a record " + record, e);
      commit(record);
    }
  }

  private void commit(ConsumerRecord<K, V> record) {
    if (!enableAutoCommit) {
      logger.info("Committing the record {}...", record);
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      offsets.put(new TopicPartition(record.topic(), record.partition()),
          new OffsetAndMetadata(record.offset() + 1));
      consumer.commitSync(offsets);
    }
  }
}
