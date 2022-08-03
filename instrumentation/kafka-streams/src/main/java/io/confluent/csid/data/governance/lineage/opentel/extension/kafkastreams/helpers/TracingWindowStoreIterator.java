/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers;
import java.util.function.Supplier;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Tracing WindowStoreIterator wrapper - used by {@link TracingWindowStore}
 * <p>
 * Implements {@link WindowStoreIterator} by delegating method calls to wrapped WindowStoreIterator
 * optionally executing tracing behaviour
 * <p>
 * Wraps operations to execute tracing handling
 * <p>
 * Extends {@link TracingKeyValueIterator}
 */
public class TracingWindowStoreIterator extends TracingKeyValueIterator<Long> implements
    WindowStoreIterator<byte[]> {

  public TracingWindowStoreIterator(KeyValueIterator<Long, byte[]> wrapped,
      StateStorePropagationHelpers stateStorePropagationHelpers,
      OpenTelemetryWrapper openTelemetryWrapper, String storeName,
      Supplier<Headers> headersAccessor) {
    super(wrapped, stateStorePropagationHelpers, openTelemetryWrapper, storeName, headersAccessor);
  }

  @Override
  public void close() {
    wrapped.close();
  }
}


