/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons.headersHandler;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons.spanHandler;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeadersHolder;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ServiceMetadata;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessTracing;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessWrapper;
import io.opentelemetry.javaagent.instrumentation.kafkaclients.TracingIterator;
import java.util.Iterator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Based on OpenTelemetry kafka-clients {@link TracingIterator}.
 * <p>
 * Wraps ConsumerRecord iterator and executes header capture logic on "next()" call.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class HeaderCapturingIterator<K, V> implements Iterator<ConsumerRecord<K, V>>,
    KafkaClientsConsumerProcessWrapper<Iterator<ConsumerRecord<K, V>>> {

  private final Iterator<ConsumerRecord<K, V>> delegateIterator;
  private final ServiceMetadata serviceMetadata;

  /**
   * Wraps iterator with {@link HeaderCapturingIterator} if
   * {@link KafkaClientsConsumerProcessTracing#wrappingEnabled()} is true.
   *
   * @param delegate generic ConsumerRecord iterator to wrap
   * @param <K>      ConsumerRecord Key type
   * @param <V>      ConsumerRecord Value type
   * @param serviceMetadata metadata that we want to pass through into the iterators for capturing
   *                        in spans created
   * @return {@link HeaderCapturingIterator}
   */
  public static <K, V> Iterator<ConsumerRecord<K, V>> wrap(
      Iterator<ConsumerRecord<K, V>> delegate, ServiceMetadata serviceMetadata) {
    if (KafkaClientsConsumerProcessTracing.wrappingEnabled()) {
      return new HeaderCapturingIterator<>(delegate, serviceMetadata);
    }
    return delegate;
  }

  @Override
  public boolean hasNext() {
    return delegateIterator.hasNext();
  }

  /**
   * In addition to returning next ConsumerRecord (if present) - store configured headers in
   * {@link HeadersHolder} for automatic propagation on produce.
   * <p>
   * Capture header key/values as Span attributes according to configured whitelist. Headers are
   * captured according to configuration as is (as byte[] values) and recorded to the consume span
   * assuming string values.
   *
   * @return next ConsumerRecord
   */
  @Override
  public ConsumerRecord<K, V> next() {
    ConsumerRecord<K, V> record = delegateIterator.next();
    if (record != null) {
      headersHandler().storeHeadersForPropagation(record.headers());
      headersHandler().captureWhitelistedHeadersAsAttributesToCurrentSpan(
          record.headers().toArray());
      spanHandler().captureServiceMetadataToSpan(serviceMetadata);
    } else {
      HeadersHolder.clear();
    }
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
