/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * Tracing KeyValueIterator wrapper - used by {@link TracingKeyValueStore}, {@link
 * TracingSessionStore} and {@link TracingWindowStoreIterator}
 * <p>
 * Implements {@link KeyValueIterator} by delegating method calls to wrapped KeyValueIterator
 * optionally executing tracing behaviour
 * <p>
 * Wraps operations to execute tracing handling
 *
 * @param <K> Key type
 */
public class TracingKeyValueIterator<K> implements KeyValueIterator<K, byte[]> {

  protected final KeyValueIterator<K, byte[]> wrapped;
  protected final StateStorePropagationHelpers stateStorePropagationHelpers;
  protected final OpenTelemetryWrapper openTelemetryWrapper;
  protected final String storeName;
  protected final ProcessorContext context;

  protected TracingKeyValueIterator(KeyValueIterator<K, byte[]> wrapped,
      StateStorePropagationHelpers stateStorePropagationHelpers,
      OpenTelemetryWrapper openTelemetryWrapper, String storeName, ProcessorContext context) {
    this.stateStorePropagationHelpers = stateStorePropagationHelpers;
    this.openTelemetryWrapper = openTelemetryWrapper;
    this.wrapped = wrapped;
    this.storeName = storeName;
    this.context = context;
  }

  @Override
  public void close() {
    wrapped.close();
  }

  @Override
  public K peekNextKey() {
    return wrapped.peekNextKey();
  }

  @Override
  public boolean hasNext() {
    return wrapped.hasNext();
  }

  @Override
  public KeyValue<K, byte[]> next() {
    KeyValue<K, byte[]> keyValue = wrapped.next();
    byte[] bytesValue = keyValue.value;
    if (null == bytesValue) {
      return keyValue;
    }
    bytesValue = stateStorePropagationHelpers.handleStateStoreGetTrace(storeName, bytesValue,
        context.headers());
    return new KeyValue<>(keyValue.key, bytesValue);
  }
}
