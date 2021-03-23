package com.volmyr.message_bus.consumer.kafka;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.consumer.MessageConsumer;
import com.volmyr.message_bus.consumer.MessageConsumerException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of {@link MessageConsumer} for Kafka platform.
 */
public abstract class KafkaStringConsumer implements MessageConsumer {

  private static final Logger logger = LoggerFactory.getLogger(KafkaStringConsumer.class);

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ImmutableList<String> topics;
  private final int pollDurationMs;
  private final boolean enableAutoCommit;
  private final KafkaConsumer<String, String> consumer;

  protected KafkaStringConsumer(
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
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollDurationMs));
        if (!records.isEmpty()) {
          logger.info("Consumer {} got {}, start handling...", this, records.count());
        } else {
          logger.debug("Consumer {} got not records", this);
        }
        for (ConsumerRecord<String, String> record : records) {
          logger.info("Start handling a record {}...", record);
          handleRecord(record);
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
  public void commit() {
    if (!enableAutoCommit) {
      consumer.commitSync();
    }
  }

  @Override
  public void shutdown() {
    closed.set(true);
    consumer.wakeup();
  }

  private void handleRecord(ConsumerRecord<String, String> record) {
    try {
      handle((MessageEvent) fromJson(record.value(), MessageEvent.newBuilder()));
    } catch (MessageConsumerException e) {
      logger.error("Error handling a record " + record, e);
    } catch (Exception e) {
      logger.error("Error handling a record " + record, e);
      commit();
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