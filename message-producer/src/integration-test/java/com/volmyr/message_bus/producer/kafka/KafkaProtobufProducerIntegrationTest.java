package com.volmyr.message_bus.producer.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import com.google.protobuf.Value;
import com.volmyr.message_bus.MessageEvent;
import com.volmyr.message_bus.MessageEventType;
import com.volmyr.message_bus.producer.MessageProducerException;
import com.volmyr.message_bus.producer.MessageProducerResponse;
import com.volmyr.message_bus.producer.ResponseType;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link KafkaProtobufProducer}.
 */
public class KafkaProtobufProducerIntegrationTest {

  private static final KafkaProtobufProducer PRODUCER = new KafkaProtobufProducer(
      "topic1",
      50_000,
      KafkaMessageProducerConfig.newBuilder()
          .setBootstrapServers("localhost:9093")
          .setAcks("all")
          .setLingerMs(1)
          .setKeySerializer("org.apache.kafka.common.serialization.StringSerializer")
          .setValueSerializer("org.apache.kafka.common.serialization.ByteArraySerializer")
          .build());

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
    assertThat(response.getKafkaResponse().getTopic()).isNotEmpty();
  }

  @AfterAll
  static void tearDown() {
    PRODUCER.close();
  }
}
