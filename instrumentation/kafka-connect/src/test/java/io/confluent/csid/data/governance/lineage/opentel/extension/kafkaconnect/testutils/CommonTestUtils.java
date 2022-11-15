/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CHARSET_UTF_8;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.TestConstants.TIMEOUTS.CONSUME_AWAIT_TIMEOUT;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.TestConstants.TIMEOUTS.DEFAULT_AWAIT_TIMEOUT;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.TestConstants.TIMEOUTS.POLL_INTERVAL;
import static java.util.Collections.singleton;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.tools.VerifiableSinkConnector;
import org.apache.kafka.connect.tools.VerifiableSinkTask;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@RequiredArgsConstructor
public class CommonTestUtils {

  private static final String KAFKA_CONTAINER_VERSION = "7.0.1";

  private static final String CONNECT_TEMP_FILE = "connect_temp_file";

  public static final Network DOCKER_NETWORK = Network.newNetwork();

  private final String tempDir;

  private String kafkaBootstrapServers = "dummy";
  private KafkaContainer kafkaContainer;

  public void startKafkaContainer() {
    if (kafkaContainer == null) {
      kafkaContainer = new KafkaContainer(
          DockerImageName.parse("confluentinc/cp-kafka:" + KAFKA_CONTAINER_VERSION))
          .withNetworkAliases("kafka")
          .withNetwork(DOCKER_NETWORK)
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500").withReuse(false);
    }
    kafkaContainer.start();
    kafkaBootstrapServers = kafkaContainer.getBootstrapServers();
  }

  public void stopKafkaContainer() {
    if (kafkaContainer != null) {
      kafkaContainer.stop();
    }
  }

  public Properties getConnectWorkerProperties() {
    Properties props = new Properties();
    props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
        createTempFile(tempDir).getAbsolutePath());
    return props;
  }

  public Properties getSourceTaskProperties(Properties overrides, String topic,
      Class<?> connectorClass) {
    Properties props = new Properties();
    props.put(ConnectorConfig.NAME_CONFIG, "VerifiableSourceTask1");
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
    props.put(VerifiableSourcePairSendingTask.TOPIC_CONFIG, topic);
    props.put(VerifiableSourcePairSendingTask.THROUGHPUT_CONFIG, "1");

    props.putAll(Optional.ofNullable(overrides).orElse(new Properties()));
    return props;
  }

  public Properties getSinkTaskProperties(Properties overrides, String topic) {
    Properties props = new Properties();
    props.put(ConnectorConfig.NAME_CONFIG, "VerifiableSinkTask1");
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, VerifiableSinkConnector.class.getName());
    props.put(VerifiableSinkTask.TOPICS_CONFIG, topic);
    props.putAll(Optional.ofNullable(overrides).orElse(new Properties()));
    return props;
  }

  public Properties getHeaderInjectTrasnformProperties() {
    Properties props = new Properties();

    props.put("transforms", "insertHeader");
    props.put("transforms.insertHeader.type", InsertHeaderBytes.class.getName());
    props.put("transforms.insertHeader.header", CAPTURED_PROPAGATED_HEADER.key());
    props.put("transforms.insertHeader.value.literal",
        new String(CAPTURED_PROPAGATED_HEADER.value(), CHARSET_UTF_8));
    return props;
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

  public String getClusterId() {
    final String[] clusterId = new String[1];
    waitUntil(
        () -> {
          try {
            clusterId[0] = getClusterIdInternal();
          } catch (ExecutionException e) {
            //Suppress - try again.
          } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted fetching clusterId", e);
          }
          return clusterId[0] != null && clusterId[0].length() > 0;
        });
    return clusterId[0];
  }

  private String getClusterIdInternal() throws ExecutionException, InterruptedException {
    try (AdminClient adminClient = KafkaAdminClient.create(getKafkaProperties(new Properties()))) {
      return adminClient.describeCluster().clusterId().get();
    }
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
      await().atMost(CONSUME_AWAIT_TIMEOUT).pollInterval(Duration.ofMillis(200)).until(() -> {
        ConsumerRecords records = consumer.poll(Duration.ofMillis(200));
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

  public static void assertTracesCaptured(List<List<SpanData>> traces,
      TraceAssertData... expectations) {
    TracesAssert.assertThat(traces).hasSize(expectations.length)
        .hasTracesSatisfyingExactly(expectations);
  }

  public File createTempFile(String tempDir) {
    return new File(tempDir, CONNECT_TEMP_FILE);
  }

  public void waitUntil(Supplier<Boolean> condition) {
    await().atMost(DEFAULT_AWAIT_TIMEOUT).pollInterval(POLL_INTERVAL).until(condition::get);
  }
}
