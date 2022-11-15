/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ServiceMetadata;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessTracing;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessWrapper;
import io.opentelemetry.javaagent.instrumentation.kafkaclients.TracingIterable;
import java.util.Iterator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Based on OpenTelemetry kafka-clients {@link TracingIterable}.
 * <p>
 * Wraps ConsumerRecord iterable to allow returning of wrapped Iterator with tracing / header
 * capture logic for header propagation
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class HeaderCapturingIterable<K, V> implements Iterable<ConsumerRecord<K, V>>,
    KafkaClientsConsumerProcessWrapper<Iterable<ConsumerRecord<K, V>>> {

  private final Iterable<ConsumerRecord<K, V>> delegate;
  private final ServiceMetadata serviceMetadata;

  /**
   * Wraps iterable with {@link HeaderCapturingIterable} if
   * {@link KafkaClientsConsumerProcessTracing#wrappingEnabled()} is true.
   *
   * @param delegate        generic ConsumerRecord iterable to wrap
   * @param <K>             ConsumerRecord Key type
   * @param <V>             ConsumerRecord Value type
   * @param serviceMetadata metadata that we want to pass through into the iterators for capturing
   *                        in spans created
   * @return {@link HeaderCapturingIterable} or wrapped iterable if wrapping is not enabled.
   */
  public static <K, V> Iterable<ConsumerRecord<K, V>> wrap(
      Iterable<ConsumerRecord<K, V>> delegate, ServiceMetadata serviceMetadata) {

    if (KafkaClientsConsumerProcessTracing.wrappingEnabled()) {
      return new HeaderCapturingIterable<>(delegate, serviceMetadata);
    }
    return delegate;
  }

  /**
   * Wraps iterator with HeaderCapturingIterator on first call, subsequent calls return already
   * wrapped iterator.
   * <p>
   * Not thread-safe.
   *
   * @return {@link HeaderCapturingIterator}
   */
  @Override
  public Iterator<ConsumerRecord<K, V>> iterator() {
    Iterator<ConsumerRecord<K, V>> it;
    // This is not thread-safe, but usually the first (hopefully only) traversal of
    // ConsumerRecords is performed in the same thread that called poll()
    it = HeaderCapturingIterator.wrap(delegate.iterator(), serviceMetadata);
    return it;
  }

  /**
   * Unwraps inner iterable - to allow disabling Kafka Clients added tracing logic when used in
   * Kafka Streams.
   * <p>
   * If inner iterable is a wrapper as well (which in most cases it will be as we are wrapping
   * {@link TracingIterable} that is in turn a wrapper) - perform double unwrap to original Kafka
   * ConsumerRecord iterable.
   *
   * @return unwrapped inner iterable
   */
  @Override
  public Iterable<ConsumerRecord<K, V>> unwrap() {
    if (delegate instanceof KafkaClientsConsumerProcessWrapper) {
      return ((KafkaClientsConsumerProcessWrapper<Iterable<ConsumerRecord<K, V>>>) delegate).unwrap();
    }
    return delegate;
  }
}
