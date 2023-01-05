/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.CACHE_LAYER;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Tracing KeyValueStoreWrapper - delegates calls to wrapped {@link KeyValueStore} adding tracing
 * behaviour where appropriate.
 */
public class TracingKeyValueStore extends BaseTracingStore<KeyValueStore<Bytes, byte[]>> implements
    KeyValueStore<Bytes, byte[]> {

  private final StateStorePropagationHelpers stateStorePropagationHelpers;
  private final OpenTelemetryWrapper openTelemetryWrapper;

  public TracingKeyValueStore(StateStorePropagationHelpers stateStorePropagationHelpers,
      OpenTelemetryWrapper openTelemetryWrapper,
      KeyValueStore<Bytes, byte[]> wrapped,
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
  public void put(Bytes key, byte[] value) {
    byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(wrapped().name(),
        value, headersAccessor.get().toArray(), isCachingStore);
    wrapped().put(key, valueWithTrace);
  }


  @Override
  public byte[] putIfAbsent(Bytes key, byte[] value) {
    byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(wrapped().name(),
        value, headersAccessor.get().toArray(), isCachingStore);
    return wrapped().putIfAbsent(key, valueWithTrace);
  }

  @Override
  public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
    List<KeyValue<Bytes, byte[]>> tracedValueList = new ArrayList<>();
    for (KeyValue<Bytes, byte[]> entry : entries) {
      byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(
          wrapped().name(),
          entry.value, headersAccessor.get().toArray(), isCachingStore);
      tracedValueList.add(new KeyValue<>(entry.key, valueWithTrace));
    }
    wrapped().putAll(tracedValueList);
  }

  @Override
  public byte[] delete(Bytes key) {
    byte[] deletedValue = wrapped().delete(key);
    if (null == deletedValue) {
      return null;
    }
    deletedValue = stateStorePropagationHelpers.handleStateStoreDeleteTrace(wrapped().name(),
        deletedValue, isCachingStore);
    return deletedValue;
  }

  @Override
  public byte[] get(Bytes key) {
    byte[] bytesValue = wrapped().get(key);
    if (null == bytesValue) {
      return null;
    }
    bytesValue = stateStorePropagationHelpers.handleStateStoreGetTrace(wrapped().name(), bytesValue,
        headersAccessor.get());
    return bytesValue;
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
    KeyValueIterator<Bytes, byte[]> resultIter = wrapped().range(from, to);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
    KeyValueIterator<Bytes, byte[]> resultIter = wrapped().reverseRange(from, to);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    KeyValueIterator<Bytes, byte[]> resultIter = wrapped().all();
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseAll() {
    KeyValueIterator<Bytes, byte[]> resultIter = wrapped().reverseAll();
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(P prefix,
      PS prefixKeySerializer) {
    KeyValueIterator<Bytes, byte[]> resultIter = wrapped().prefixScan(prefix, prefixKeySerializer);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public long approximateNumEntries() {
    return wrapped().approximateNumEntries();
  }

}
