package com.volmyr.message_bus.consumer.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.MessageEventType;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link KafkaProtobufConsumer}.
 */
public class KafkaProtobufConsumerIntegrationTest {

  private static final KafkaProtobufConsumer CONSUMER = new KafkaProtobufConsumer(
      ImmutableList.of("topic1"),
      2_000,
      KafkaMessageConsumerConfig.newBuilder()
          .setBootstrapServers("localhost:9092")
          .setGroupId("group")
          .setEnableAutoCommit(true)
          .setAutoCommitIntervalMs(1000)
          .setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
          .setValueDeserializer("org.apache.kafka.common.serialization.ByteArrayDeserializer")
          .build()) {

    @Override
    public void handle(MessageEvent event) {
      assertThat(event.getType()).isEqualTo(MessageEventType.TYPE1);
      assertThat(event.getId()).isNotEmpty();
    }
  };

  @Test
  void shouldConsumeMessages() throws InterruptedException {
    Thread thread = new Thread(CONSUMER);
    thread.start();
    Thread.sleep(60_000);
    CONSUMER.shutdown();
    Thread.sleep(3_000);
  }
}