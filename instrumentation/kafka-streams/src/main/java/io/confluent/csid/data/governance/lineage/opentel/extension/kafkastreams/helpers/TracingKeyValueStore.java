/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

/**
 * Tracing KeyValueStoreWrapper - delegates calls to wrapped {@link KeyValueStore} adding tracing
 * behaviour where appropriate.
 */
public class TracingKeyValueStore extends
    WrappedStateStore<KeyValueStore<Bytes, byte[]>, Bytes, byte[]> implements
    KeyValueStore<Bytes, byte[]> {

  private final StateStorePropagationHelpers stateStorePropagationHelpers;
  private final OpenTelemetryWrapper openTelemetryWrapper;
  private ProcessorContext context;

  public TracingKeyValueStore(StateStorePropagationHelpers stateStorePropagationHelpers,
      OpenTelemetryWrapper openTelemetryWrapper,
      KeyValueStore<Bytes, byte[]> wrapped) {
    super(wrapped);
    this.stateStorePropagationHelpers = stateStorePropagationHelpers;
    this.openTelemetryWrapper = openTelemetryWrapper;
  }

  @Override
  public void put(Bytes key, byte[] value) {
    byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(wrapped().name(),
        value, context.headers().toArray());
    wrapped().put(key, valueWithTrace);
  }


  @Override
  public byte[] putIfAbsent(Bytes key, byte[] value) {
    byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(wrapped().name(),
        value, context.headers().toArray());
    return wrapped().putIfAbsent(key, valueWithTrace);
  }

  @Override
  public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
    List<KeyValue<Bytes, byte[]>> tracedValueList = new ArrayList<>();
    for (KeyValue<Bytes, byte[]> entry : entries) {
      byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(
          wrapped().name(),
          entry.value, context.headers().toArray());
      tracedValueList.add(new KeyValue<>(entry.key, valueWithTrace));
    }
    wrapped().putAll(tracedValueList);
  }

  @Override
  public byte[] delete(Bytes key) {
    byte[] deletedValue = wrapped().delete(key);
    if (null == deletedValue) {
      return deletedValue;
    }
    deletedValue = stateStorePropagationHelpers.handleStateStoreDeleteTrace(wrapped().name(),
        deletedValue);
    return deletedValue;
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    this.context = context;
    wrapped().init(context, root);
  }

  @Override
  public byte[] get(Bytes key) {
    byte[] bytesValue = wrapped().get(key);
    if (null == bytesValue) {
      return null;
    }
    bytesValue = stateStorePropagationHelpers.handleStateStoreGetTrace(wrapped().name(), bytesValue,
        context.headers());
    return bytesValue;
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
    KeyValueIterator<Bytes, byte[]> resultIter = wrapped().range(from, to);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), context);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    KeyValueIterator<Bytes, byte[]> resultIter = wrapped().all();
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), context);
  }

  @Override
  public long approximateNumEntries() {
    return wrapped().approximateNumEntries();
  }

}
