/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static java.util.Collections.singleton;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class CommonTestUtils {

  private String kafkaBootstrapServers = "dummy";
  private KafkaContainer kafkaContainer;

  public void startKafkaContainer() {
    if (kafkaContainer == null) {
      kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.0.1"))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500").withReuse(true);
    }
    kafkaContainer.start();
    kafkaBootstrapServers = kafkaContainer.getBootstrapServers();
  }

  public void stopKafkaContainer() {
    if (kafkaContainer != null) {
      kafkaContainer.stop();
    }
  }

  public Properties getKafkaProperties(Properties overrides) {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer-" + UUID.randomUUID());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
    props.putAll(overrides);
    return props;
  }

  public Properties getPropertiesForStreams() {
    Properties props = new Properties();
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-kstream-app" + UUID.randomUUID());
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); // disable ktable cache
    return props;
  }

  public void produceSingleEvent(String topic, String key, String value, Header... headers) {
    produceSingleEvent(topic, key, value, headers, StringSerializer.class, StringSerializer.class);
  }

  public void produceSingleEvent(String topic, Integer key, Integer value, Header... headers) {
    produceSingleEvent(topic, key, value, headers, IntegerSerializer.class,
        IntegerSerializer.class);
  }

  public void produceSingleEvent(
      String topic, Object key, Object value, Header[] headers, final Class<?> keySerializerClass,
      final Class<?> valueSerializerClass) {
    Properties overrides = new Properties();
    overrides.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
    overrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);

    KafkaProducer kafkaProducer = new KafkaProducer<>(
        getKafkaProperties(overrides));
    ProducerRecord producerRecord = new ProducerRecord<>(topic, key, value);
    Arrays.stream(headers).forEach(header -> producerRecord.headers().add(header));

    kafkaProducer.send(producerRecord);
    kafkaProducer.flush();
    kafkaProducer.close();
    log.info("Produced Records: {}", producerRecord);
  }

  public List<ConsumerRecord> consumeAtLeastXEvents(final Class<?> keyDeserializerClass,
      final Class<?> valueDeserializerClass, String topic, int minimumNumberOfEventsToConsume) {
    Properties overrides = new Properties();
    return consumeAtLeastXEvents(keyDeserializerClass, valueDeserializerClass, topic,
        minimumNumberOfEventsToConsume, overrides);
  }

  public List<ConsumerRecord> consumeAtLeastXEvents(final Class<?> keyDeserializerClass,
      final Class<?> valueDeserializerClass, String topic, int minimumNumberOfEventsToConsume,
      Properties overrides) {

    List<ConsumerRecord> consumed = new ArrayList<>();
    try {
      overrides.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
      overrides.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
      KafkaConsumer consumer = new KafkaConsumer(getKafkaProperties(overrides));
      consumer.subscribe(singleton(topic));
      await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).until(() -> {
        ConsumerRecords records = consumer.poll(Duration.ofMillis(900));
        Iterator<ConsumerRecord> recordIterator = records.iterator();
        while (recordIterator.hasNext()) {
          consumed.add(recordIterator.next());
        }
        return consumed.size() >= minimumNumberOfEventsToConsume;
      });
      consumer.commitSync();
      consumer.close();
    } catch (Exception e) {
      log.info("Consumed Records: {}",
          consumed.stream().map(event -> Pair.of(event.key(), event.value())).collect(
              Collectors.toList()));
      throw e;
    }
    log.info("Consumed Records: {}",
        consumed.stream().map(event -> Pair.of(event.key(), event.value())).collect(
            Collectors.toList()));
    return consumed;
  }

  public ConsumerRecord consumeEvent(final Class<?> keyDeserializerClass,
      final Class<?> valueDeserializerClass, String topic) {
    return consumeAtLeastXEvents(keyDeserializerClass, valueDeserializerClass, topic, 1).get(0);
  }

  static void assertTracesCaptured(List<List<SpanData>> traces, TraceAssertData... expectations) {
    TracesAssert.assertThat(traces).hasSize(expectations.length)
        .hasTracesSatisfyingExactly(expectations);
  }

  public void createTopologyAndStartKStream(KafkaStreams kafkaStreams, CountDownLatch streamsLatch,
      String... topics) {
    if (topics != null && topics.length > 0) {
      AdminClient adminClient = KafkaAdminClient.create(
          getKafkaProperties(new Properties()));
      adminClient.createTopics(Arrays.stream(topics).map(topic ->
          new NewTopic(topic, 1, (short) 1)).collect(Collectors.toList()));
      adminClient.close();
    }

    new Thread(() -> {
      kafkaStreams.start();
      try {
        streamsLatch.await();
        kafkaStreams.close();
        kafkaStreams.cleanUp();
      } catch (InterruptedException e) {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
        Thread.currentThread().interrupt();
      }
    }).start();
  }

  public void awaitKStreamsShutdown(KafkaStreams kafkaStreams) {
    await().atMost(Duration.ofSeconds(60)).until(()->kafkaStreams.state().equals(State.NOT_RUNNING));
  }
}
