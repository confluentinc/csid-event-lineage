/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static java.util.Collections.singleton;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class CommonTestUtils {

  private static final String KAFKA_CONTAINER_VERSION = "7.0.1";

  private String kafkaBootsrapServers;
  private KafkaContainer kafkaContainer;
  private KafkaProducer<String, String> kafkaProducer;

  public CommonTestUtils() {
  }

  public void startKafkaContainer() {
    if (kafkaContainer == null) {
      kafkaContainer = new KafkaContainer(
          DockerImageName.parse("confluentinc/cp-kafka:" + KAFKA_CONTAINER_VERSION))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500").withReuse(true);
    }
    kafkaContainer.start();
    kafkaBootsrapServers = kafkaContainer.getBootstrapServers();
    kafkaProducer = new KafkaProducer<>(getKafkaProperties());
  }

  public void stopKafkaContainer() {
    kafkaProducer.close();
    if (kafkaContainer != null) {
      kafkaContainer.stop();
    }
  }

  public Properties getKafkaProperties() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer-" + UUID.randomUUID());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootsrapServers);

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
    return props;
  }

  public void produceSingleEvent(
      String topic, String key, String value, Header... headers) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
    if (isNotEmpty(headers)) {
      Arrays.stream(headers).forEach(header -> producerRecord.headers().add(header));
    }
    kafkaProducer.send(producerRecord);
    kafkaProducer.flush();
    log.info("Produced Records: {}", producerRecord);
  }

  public ConsumerRecord<String, String> consumeEvent(String topic) {
    List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
    consumeEvents(topic, 1, consumerRecords::add);
    return consumerRecords.get(0);
  }

  public void consumeEvents(String topic, int numberToConsume,
      Consumer<ConsumerRecord<String, String>> consumedRecordProcessFunc) {
    List<ConsumerRecord<String, String>> consumed = new ArrayList<>();

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaProperties());

    consumer.subscribe(singleton(topic));
    await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).until(() -> {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(900));
      // Noop - need to iterate through received records to kick off Process span
      // and capture consumed records for assertions
      records.forEach(record -> {
        consumed.add(record);
        if (consumedRecordProcessFunc != null) {
          consumedRecordProcessFunc.accept(record);
        }

      });
      return consumed.size() == numberToConsume;
    });
    consumer.close();
    log.info("Consumed Records: {}", consumed);
  }

  static void assertTracesCaptured(List<List<SpanData>> traces, TraceAssertData... expectations) {
    TracesAssert.assertThat(traces).hasSize(expectations.length)
        .hasTracesSatisfyingExactly(expectations);
  }
}
