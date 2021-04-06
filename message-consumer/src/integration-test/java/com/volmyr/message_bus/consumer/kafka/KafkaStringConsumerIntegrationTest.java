package com.volmyr.message_bus.consumer.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.Value;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.MessageEventType;
import com.volmyr.message_bus.consumer.MessageConsumerException;
import com.volmyr.message_bus.consumer.kafka.KafkaMessageConsumerConfig.AutoOffsetReset;
import com.volmyr.message_bus.producer.MessageProducerException;
import com.volmyr.message_bus.producer.MessageProducerResponse;
import com.volmyr.message_bus.producer.ResponseType;
import com.volmyr.message_bus.producer.kafka.KafkaMessageProducerConfig;
import com.volmyr.message_bus.producer.kafka.KafkaStringProducer;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests for {@link KafkaStringConsumer}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaStringConsumerIntegrationTest {

  private static final String KAFKA_SERVERS = "localhost:9093";
  private static final String TOPIC = UUID.randomUUID().toString();

  private static final KafkaStringProducer PRODUCER = new KafkaStringProducer(
      TOPIC,
      50_000,
      KafkaMessageProducerConfig.newBuilder()
          .setBootstrapServers(KAFKA_SERVERS)
          .setAcks("all")
          .setLingerMs(1)
          .setKeySerializer("org.apache.kafka.common.serialization.StringSerializer")
          .setValueSerializer("org.apache.kafka.common.serialization.StringSerializer")
          .build());

  private static final AdminClient ADMIN_CLIENT = KafkaAdminClient.create(
      ImmutableMap.of("bootstrap.servers", KAFKA_SERVERS));

  private static final KafkaStringConsumer CONSUMER = spy(new KafkaStringConsumerImpl(
      ImmutableList.of(TOPIC),
      2_000,
      KafkaMessageConsumerConfig.newBuilder()
          .setBootstrapServers(KAFKA_SERVERS)
          .setGroupId("group")
          .setEnableAutoCommit(false)
          //.setAutoCommitIntervalMs(1000)
          .setAutoOffsetReset(AutoOffsetReset.EARLIEST)
          .setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
          .setValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer")
          .build()));

  private static final MessageEvent EVENT = MessageEvent.newBuilder()
      .setType(MessageEventType.TYPE1)
      .setCreatedTimestamp(new Date().getTime())
      .setId(UUID.randomUUID().toString())
      .setStringData("Hello Kafka")
      .setAnyData(Any.pack(Value.newBuilder().setStringValue("Hello Kafka").build()))
      .build();

  @Test
  @Order(1)
  void shouldSendMessage() throws MessageProducerException {
    MessageProducerResponse response = PRODUCER.send(EVENT);

    assertThat(response.getType()).isEqualTo(ResponseType.KAFKA);
    assertThat(response.getKafkaResponse().getTopic()).isNotEmpty();
    assertThat(response.getKafkaResponse().getSerializedValueSize()).isGreaterThan(1);
  }

  @Test
  @Order(2)
  void assertTopicExists() throws InterruptedException, ExecutionException, TimeoutException {
    KafkaFuture<Set<String>> topicsFuture = ADMIN_CLIENT.listTopics().names();
    Set<String> topics = topicsFuture.get(1, TimeUnit.MINUTES);

    assertThat(topics.contains(TOPIC)).isTrue();
  }

  @Test
  @Order(3)
  void shouldConsumeMessages() throws InterruptedException, MessageConsumerException {
    Thread thread = new Thread(CONSUMER);
    thread.start();
    Thread.sleep(10_000);
    CONSUMER.shutdown();
    verify(CONSUMER, times(10)).handle(EVENT);
    Thread.sleep(1_000);
  }

  @AfterAll
  static void tearDown() {
    PRODUCER.close();
    ADMIN_CLIENT.close();
    CONSUMER.shutdown();
  }

  static class KafkaStringConsumerImpl extends KafkaStringConsumer {

    private int counter;

    KafkaStringConsumerImpl(
        ImmutableList<String> topics, int pollDurationMs, KafkaMessageConsumerConfig config) {
      super(topics, pollDurationMs, config);
    }

    @Override
    public void handle(MessageEvent event) throws MessageConsumerException {
      assertThat(event.getType()).isEqualTo(MessageEventType.TYPE1);
      assertThat(event.getId()).isNotEmpty();
      counter++;
      if (counter < 10) {
        throw new MessageConsumerException("Counter less 10 but is " + counter);
      }
    }
  }
}
