package com.volmyr.message_bus.consumer.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.MessageEventType;
import com.volmyr.message_bus.consumer.MessageConsumerException;
import com.volmyr.message_bus.consumer.kafka.KafkaMessageConsumerConfig.AutoOffsetReset;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link KafkaProtobufConsumer}.
 */
public class KafkaProtobufConsumerIntegrationTest {

  private static final KafkaProtobufConsumer CONSUMER = new KafkaProtobufConsumer(
      ImmutableList.of("topic1"),
      10_000,
      KafkaMessageConsumerConfig.newBuilder()
          .setBootstrapServers("localhost:9093")
          .setGroupId("group")
          //.setEnableAutoCommit(true)
          //.setAutoCommitIntervalMs(1000)
          .setAutoOffsetReset(AutoOffsetReset.EARLIEST)
          .setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
          .setValueDeserializer("org.apache.kafka.common.serialization.ByteArrayDeserializer")
          .build()) {

    private int counter;

    @Override
    public void handle(MessageEvent event) throws MessageConsumerException {
      assertThat(event.getType()).isEqualTo(MessageEventType.TYPE1);
      assertThat(event.getId()).isNotEmpty();
      counter++;
      if (counter < 10) {
        throw new MessageConsumerException("Counter less 10 but is " + counter);
      }
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
