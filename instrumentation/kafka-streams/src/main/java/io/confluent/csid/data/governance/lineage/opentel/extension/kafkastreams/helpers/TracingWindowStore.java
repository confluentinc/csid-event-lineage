/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.CACHE_LAYER;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Tracing WindowStore - delegates calls to wrapped {@link WindowStore} adding tracing behaviour
 * where appropriate.
 */
public class TracingWindowStore extends
    BaseTracingStore<WindowStore<Bytes, byte[]>> implements
    WindowStore<Bytes, byte[]> {

  private final StateStorePropagationHelpers stateStorePropagationHelpers;
  private final OpenTelemetryWrapper openTelemetryWrapper;

  public TracingWindowStore(StateStorePropagationHelpers stateStorePropagationHelpers,
      OpenTelemetryWrapper openTelemetryWrapper,
      WindowStore<Bytes, byte[]> wrapped,
      CACHE_LAYER isCachingStore) {
    super(wrapped, isCachingStore);
    this.stateStorePropagationHelpers = stateStorePropagationHelpers;
    this.openTelemetryWrapper = openTelemetryWrapper;
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    wrapped().init(context, root);
  }

  @Override
  public void init(StateStoreContext context, StateStore root) {
    wrapped().init(context, root);
  }

  @Override
  public void put(Bytes key, byte[] value, long windowStartTimestamp) {
    byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(wrapped().name(),
        value, headersAccessor.get().toArray(), isCachingStore);
    wrapped().put(key, valueWithTrace, windowStartTimestamp);
  }

  @Override
  public byte[] fetch(Bytes key, long time) {
    byte[] bytesValue = wrapped().fetch(key, time);
    if (null == bytesValue) {
      return null;
    }
    bytesValue = stateStorePropagationHelpers.handleStateStoreGetTrace(wrapped().name(), bytesValue,
        headersAccessor.get());
    return bytesValue;
  }

  @Override
  public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {
    WindowStoreIterator<byte[]> resultIter = wrapped().fetch(key, timeFrom, timeTo);
    return new TracingWindowStoreIterator(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public WindowStoreIterator<byte[]> backwardFetch(Bytes key, long timeFrom, long timeTo) {
    WindowStoreIterator<byte[]> resultIter = wrapped().backwardFetch(key, timeFrom, timeTo);
    return new TracingWindowStoreIterator(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to, long timeFrom,
      long timeTo) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().fetch(from, to, timeFrom,
        timeTo);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(Bytes from, Bytes to, long timeFrom,
      long timeTo) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().backwardFetch(from, to, timeFrom,
        timeTo);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().all();
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().backwardAll();
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().fetchAll(timeFrom, timeTo);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(long timeFrom, long timeTo) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().backwardFetchAll(timeFrom, timeTo);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }
}
