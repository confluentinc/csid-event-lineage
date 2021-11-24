/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessTracing;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessWrapper;
import io.opentelemetry.javaagent.instrumentation.kafkaclients.TracingIterable;
import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Based on OpenTelemetry kafka-clients {@link TracingIterable}.
 * <p>
 * Wraps ConsumerRecord iterable to allow returning of wrapped Iterator with tracing / payload
 * capture logic
 */
public class PayloadCapturingIterable<K, V>
    implements Iterable<ConsumerRecord<K, V>>,
    KafkaClientsConsumerProcessWrapper<Iterable<ConsumerRecord<K, V>>> {

  private final Iterable<ConsumerRecord<K, V>> delegate;
  private boolean firstIterator = true;

  protected PayloadCapturingIterable(
      Iterable<ConsumerRecord<K, V>> delegate) {
    this.delegate = delegate;
  }

  /**
   * Wraps iterable with PayloadCapturingIterable if {@link KafkaClientsConsumerProcessTracing#wrappingEnabled()}
   * is true.
   *
   * @param delegate generic ConsumerRecord iterable to wrap
   * @param <K>      ConsumerRecord Key type
   * @param <V>      ConsumerRecord Value type
   * @return PayloadCapturingIterable
   */
  public static <K, V> Iterable<ConsumerRecord<K, V>> wrap(
      Iterable<ConsumerRecord<K, V>> delegate) {
    if (KafkaClientsConsumerProcessTracing.wrappingEnabled()) {
      return new PayloadCapturingIterable<>(delegate);
    }
    return delegate;
  }

  /**
   * Wraps iterator with PayloadCapturingIterator on first call, subsequent calls return already
   * wrapped iterator.
   * <p>
   * Not thread-safe.
   *
   * @return {@link PayloadCapturingIterator}
   */
  @Override
  public Iterator<ConsumerRecord<K, V>> iterator() {
    Iterator<ConsumerRecord<K, V>> it;
    // We should only return one iterator with tracing.
    // However, this is not thread-safe, but usually the first (hopefully only) traversal of
    // ConsumerRecords is performed in the same thread that called poll()
    if (firstIterator) {
      it = PayloadCapturingIterator.wrap(delegate.iterator());
      firstIterator = false;
    } else {
      it = delegate.iterator();
    }

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
