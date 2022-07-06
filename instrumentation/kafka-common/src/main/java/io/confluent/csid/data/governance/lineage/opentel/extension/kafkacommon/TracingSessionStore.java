/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

/**
 * Tracing SessionStore - delegates calls to wrapped {@link SessionStore} adding tracing behaviour
 * where appropriate.
 */
public class TracingSessionStore extends
    BaseTracingStore<SessionStore<Bytes, byte[]>> implements
    SessionStore<Bytes, byte[]> {

  private final StateStorePropagationHelpers stateStorePropagationHelpers;
  private final OpenTelemetryWrapper openTelemetryWrapper;

  public TracingSessionStore(StateStorePropagationHelpers stateStorePropagationHelpers,
      OpenTelemetryWrapper openTelemetryWrapper,
      SessionStore<Bytes, byte[]> wrapped) {
    super(wrapped);
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
  public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key,
      long earliestSessionEndTime, long latestSessionStartTime) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().findSessions(key,
        earliestSessionEndTime, latestSessionStartTime);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom, Bytes keyTo,
      long earliestSessionEndTime, long latestSessionStartTime) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().findSessions(keyFrom, keyTo,
        earliestSessionEndTime, latestSessionStartTime);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public byte[] fetchSession(Bytes key, long startTime, long endTime) {
    byte[] bytesValue = wrapped().fetchSession(key, startTime, endTime);
    if (null == bytesValue) {
      return null;
    }
    bytesValue = stateStorePropagationHelpers.handleStateStoreGetTrace(wrapped().name(), bytesValue,
        headersAccessor.get());
    return bytesValue;
  }

  @Override
  public void remove(Windowed<Bytes> sessionKey) {
    stateStorePropagationHelpers.handleStateStoreSessionRemoveSpan(wrapped().name(),
        headersAccessor.get().toArray());
    wrapped().remove(sessionKey);
  }

  @Override
  public void put(Windowed<Bytes> sessionKey, byte[] aggregate) {
    byte[] valueWithTrace = stateStorePropagationHelpers.handleStateStorePutTrace(wrapped().name(),
        aggregate, headersAccessor.get().toArray());
    wrapped().put(sessionKey, valueWithTrace);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().fetch(key);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to) {
    KeyValueIterator<Windowed<Bytes>, byte[]> resultIter = wrapped().fetch(from, to);
    return new TracingKeyValueIterator<>(resultIter, stateStorePropagationHelpers,
        openTelemetryWrapper, wrapped().name(), headersAccessor);
  }
}
