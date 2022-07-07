/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ServiceMetadata;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessTracing;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessWrapper;
import io.opentelemetry.javaagent.instrumentation.kafkaclients.TracingIterator;
import io.opentelemetry.javaagent.instrumentation.kafkaclients.TracingList;
import java.util.Iterator;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Based on OpenTelemetry kafka-client {@link TracingList}.
 * <p>
 * Tracing is only implemented if using the List as Iterable but not for other List methods.
 * <p>
 * Will have to be revisited.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class HeaderCapturingList<K, V> implements List<ConsumerRecord<K, V>>,
    KafkaClientsConsumerProcessWrapper<List<ConsumerRecord<K, V>>> {

  @Delegate(excludes = Iterable.class)
  private final List<ConsumerRecord<K, V>> delegate;
  private final ServiceMetadata serviceMetadata;

  public static <K, V> List<ConsumerRecord<K, V>> wrap(List<ConsumerRecord<K, V>> delegate,
      ServiceMetadata serviceMetadata) {
    if (KafkaClientsConsumerProcessTracing.wrappingEnabled()) {
      return new HeaderCapturingList<>(delegate, serviceMetadata);
    }
    return delegate;
  }

  /**
   * Wraps iterator with {@link HeaderCapturingIterator}.
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
   * Unwraps inner iterator - to allow disabling Kafka Clients added tracing logic when used in
   * Kafka Streams.
   * <p>
   * If inner iterator is a wrapper as well (which in most cases it will be - as we are wrapping
   * {@link TracingIterator} that, is in turn, a wrapper) - perform double unwrap to original Kafka
   * ConsumerRecord iterator.
   *
   * @return unwrapped inner iterator
   */
  @Override
  public List<ConsumerRecord<K, V>> unwrap() {
    if (delegate instanceof KafkaClientsConsumerProcessWrapper) {
      return ((KafkaClientsConsumerProcessWrapper<List<ConsumerRecord<K, V>>>) delegate).unwrap();
    }
    return delegate;
  }
}
