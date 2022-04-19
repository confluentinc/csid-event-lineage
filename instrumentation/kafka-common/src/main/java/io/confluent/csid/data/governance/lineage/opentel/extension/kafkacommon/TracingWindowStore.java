/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import java.time.Instant;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

/**
 * Tracing WindowStore - delegates calls to wrapped {@link WindowStore} adding tracing behaviour
 * where appropriate.
 */
public class TracingWindowStore extends
    WrappedStateStore<WindowStore<Bytes, byte[]>, Bytes, byte[]> implements
    WindowStore<Bytes, byte[]> {

  private final StateStorePropagationHelpers stateStorePropagationHelpers;
  private final OpenTelemetryWrapper openTelemetryWrapper;
  private ProcessorContext context;

  public TracingWindowStore(StateStorePropagationHelpers stateStorePropagationHelpers,
      OpenTelemetryWrapper openTelemetryWrapper,
      WindowStore<Bytes, byte[]> wrapped) {
    super(wrapped);
    this.stateStorePropagationHelpers = stateStorePropagationHelpers;
    this.openTelemetryWrapper = openTelemetryWrapper;
  }

  @Deprecated
  @Override
  public void put(Bytes key, byte[] value) {
    byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(wrapped().name(),
        value, context.headers().toArray());
    wrapped().put(key, valueWithTrace);
  }

  @Override
  public void put(Bytes key, byte[] value, long windowStartTimestamp) {
    byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(wrapped().name(),
        value, context.headers().toArray());
    wrapped().put(key, valueWithTrace, windowStartTimestamp);
  }

  @Override
  public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {
    WindowStoreIterator<byte[]> resultIter = wrapped().fetch(key, timeFrom, timeTo);
    return new TracingWindowStoreIterator(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), context);
  }

  @Override
  public WindowStoreIterator<byte[]> fetch(Bytes key, Instant from, Instant to) {
    WindowStoreIterator<byte[]> resultIter = wrapped().fetch(key, from, to);
    return new TracingWindowStoreIterator(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), context);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to, long timeFrom,
      long timeTo) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().fetch(from, to, timeFrom,
        timeTo);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), context);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to, Instant fromTime,
      Instant toTime) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().fetch(from, to, fromTime,
        toTime);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), context);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().all();
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), context);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().fetchAll(timeFrom, timeTo);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), context);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(Instant from, Instant to) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().fetchAll(from, to);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), context);
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    this.context = context;
    wrapped().init(context, root);
  }

  @Override
  public byte[] fetch(Bytes key, long time) {
    byte[] bytesValue = wrapped().fetch(key, time);
    if (null == bytesValue) {
      return null;
    }
    bytesValue = stateStorePropagationHelpers.handleStateStoreGetTrace(wrapped().name(), bytesValue,
        context.headers());
    return bytesValue;
  }
}
