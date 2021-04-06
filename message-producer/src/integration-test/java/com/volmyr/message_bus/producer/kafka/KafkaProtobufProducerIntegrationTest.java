package com.volmyr.message_bus.producer.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.Value;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.MessageEventType;
import com.volmyr.message_bus.producer.MessageProducerException;
import com.volmyr.message_bus.producer.MessageProducerResponse;
import com.volmyr.message_bus.producer.ResponseType;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link KafkaProtobufProducer}.
 */
public class KafkaProtobufProducerIntegrationTest {

  private static final String KAFKA_SERVERS = "localhost:9093";
  private static final String TOPIC = UUID.randomUUID().toString();
  private static final int TOPIC_NUMBER_OF_PARTITIONS = 1;
  private static final short TOPIC_REPLICATION_FACTOR = 1;

  private static final KafkaProtobufProducer PRODUCER = new KafkaProtobufProducer(
      TOPIC,
      50_000,
      KafkaMessageProducerConfig.newBuilder()
          .setBootstrapServers(KAFKA_SERVERS)
          .setAcks("all")
          .setLingerMs(1)
          .setKeySerializer("org.apache.kafka.common.serialization.StringSerializer")
          .setValueSerializer("org.apache.kafka.common.serialization.ByteArraySerializer")
          .build());

  private static final AdminClient ADMIN_CLIENT = KafkaAdminClient.create(
      ImmutableMap.of("bootstrap.servers", KAFKA_SERVERS));

  @BeforeAll
  static void setUp() throws Exception {
    KafkaFuture<Set<String>> topicsFuture = ADMIN_CLIENT.listTopics().names();
    Set<String> topics = topicsFuture.get(1, TimeUnit.MINUTES);
    if (!topics.contains(TOPIC)) {
      ADMIN_CLIENT.createTopics(ImmutableList
          .of(new NewTopic(TOPIC, TOPIC_NUMBER_OF_PARTITIONS, TOPIC_REPLICATION_FACTOR)));
    }
  }

  @Test
  void shouldSendMessage() throws MessageProducerException {
    MessageProducerResponse response = PRODUCER.send(MessageEvent.newBuilder()
        .setType(MessageEventType.TYPE1)
        .setCreatedTimestamp(new Date().getTime())
        .setId(UUID.randomUUID().toString())
        .setStringData("Hello Kafka")
        .setAnyData(Any.pack(Value.newBuilder().setStringValue("Hello Kafka").build()))
        .build());
    assertThat(response.getType()).isEqualTo(ResponseType.KAFKA);
    assertThat(response.getKafkaResponse().getTopic()).isEqualTo(TOPIC);
    assertThat(response.getKafkaResponse().getSerializedValueSize()).isGreaterThan(1);
  }

  @AfterAll
  static void tearDown() {
    PRODUCER.close();
    ADMIN_CLIENT.close();
  }
}
