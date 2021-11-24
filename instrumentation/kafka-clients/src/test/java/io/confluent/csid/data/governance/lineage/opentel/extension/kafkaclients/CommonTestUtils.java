/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static java.util.Collections.singleton;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class CommonTestUtils {

  private String kafkaBootsrapServers;
  private KafkaContainer kafkaContainer;

  public CommonTestUtils() {
  }

  public void startKafkaContainer() {
    if (kafkaContainer == null) {
      kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500").withReuse(true);
    }
    kafkaContainer.start();
    kafkaBootsrapServers = kafkaContainer.getBootstrapServers();
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
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootsrapServers);

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
    props.putAll(overrides);
    return props;
  }

  public void produceSingleEvent(Class<?> keySerializerClass, Class<?> valueSerializerClass,
      String topic, Object key, Object value) {
    Properties overrides = new Properties();
    overrides.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
    overrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);

    KafkaProducer kafkaProducer = new KafkaProducer<>(getKafkaProperties(overrides));
    kafkaProducer.send(new ProducerRecord<>(topic, key, value));
    kafkaProducer.flush();
    kafkaProducer.close();
  }

  public void consumeEvent(final Class<?> keyDeserializerClass,
      final Class<?> valueDeserializerClass, String topic) {
    Properties overrides = new Properties();
    overrides.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
    overrides.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
    KafkaConsumer consumer = new KafkaConsumer(getKafkaProperties(overrides));
    consumer.subscribe(singleton(topic));
    await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).until(() -> {
      ConsumerRecords records = consumer.poll(Duration.ofMillis(900));
      records.forEach(
          record -> {/*Noop - need to iterate through received records to kick off Process span */});
      return !records.isEmpty();
    });
  }
}
