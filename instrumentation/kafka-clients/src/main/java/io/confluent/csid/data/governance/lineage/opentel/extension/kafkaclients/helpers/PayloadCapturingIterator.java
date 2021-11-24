/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.CommonUtil;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessTracing;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessWrapper;
import io.opentelemetry.javaagent.instrumentation.kafkaclients.TracingIterator;
import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Based on OpenTelemetry kafka-clients {@link TracingIterator}.
 * <p>
 * Wraps ConsumerRecord iterator and executes payload capture logic on "next()" call.
 */
public class PayloadCapturingIterator<K, V>
    implements Iterator<ConsumerRecord<K, V>>,
    KafkaClientsConsumerProcessWrapper<Iterator<ConsumerRecord<K, V>>> {

  private final Iterator<ConsumerRecord<K, V>> delegateIterator;

  private PayloadCapturingIterator(
      Iterator<ConsumerRecord<K, V>> delegateIterator) {
    this.delegateIterator = delegateIterator;

  }

  /**
   * Wraps iterator with PayloadCapturingIterator if {@link KafkaClientsConsumerProcessTracing#wrappingEnabled()}
   * is true.
   *
   * @param delegate generic ConsumerRecord iterator to wrap
   * @param <K>      ConsumerRecord Key type
   * @param <V>      ConsumerRecord Value type
   * @return PayloadCapturingIterator
   */
  public static <K, V> Iterator<ConsumerRecord<K, V>> wrap(
      Iterator<ConsumerRecord<K, V>> delegate) {
    if (KafkaClientsConsumerProcessTracing.wrappingEnabled()) {
      return new PayloadCapturingIterator<>(delegate);
    }
    return delegate;
  }

  @Override
  public boolean hasNext() {
    return delegateIterator.hasNext();
  }

  /**
   * in addition to returning next ConsumerRecord (if present) captures ConsumerRecord key and value
   * as Json and adds those to current Span attributes.
   *
   * @return next ConsumerRecord
   */
  @Override
  public ConsumerRecord<K, V> next() {
    ConsumerRecord<K, V> record = delegateIterator.next();
    CommonUtil.captureKeyValuePayloadsToSpan(record.key(), record.value(),
        Singletons.objectMapper(), Span.current());

    return record;
  }


  @Override
  public void remove() {
    delegateIterator.remove();
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
  public Iterator<ConsumerRecord<K, V>> unwrap() {
    if (delegateIterator instanceof KafkaClientsConsumerProcessWrapper) {
      return ((KafkaClientsConsumerProcessWrapper<Iterator<ConsumerRecord<K, V>>>) delegateIterator).unwrap();
    }
    return delegateIterator;
  }
}
