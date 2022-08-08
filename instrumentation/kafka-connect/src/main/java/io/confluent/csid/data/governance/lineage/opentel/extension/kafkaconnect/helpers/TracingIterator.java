/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.connectHandler;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.headerCaptureConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.openTelemetryWrapper;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.spanHandler;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.field.VirtualField;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Wraps ConnectRecord iterator and executes span creation and header capture logic on "next()"
 * call.
 */
@Slf4j
public class TracingIterator<T extends ConnectRecord<T>>
    implements Iterator<T> {

  private final Iterator<T> delegateIterator;
  private final String spanName;
  private final String connectorId;
  private Scope currentScope;
  private Span currentSpan;

  private final VirtualField<SinkRecord, Context> sinkRecordContextStore;
  private final VirtualField<SourceRecord, Context> sourceRecordContextStore;

  /**
   * Wraps delegate iterator
   *
   * @param delegateIterator iterator to wrap
   * @param spanName         Span name for creating new spans on next() call.
   * @param connectorId      connectorId - used for service name overriding during span creation
   */
  public TracingIterator(
      Iterator<T> delegateIterator, String spanName, String connectorId) {
    this.spanName = spanName;
    this.connectorId = connectorId;
    this.delegateIterator = delegateIterator;
    this.sinkRecordContextStore = VirtualField.find(SinkRecord.class, Context.class);
    this.sourceRecordContextStore = VirtualField.find(SourceRecord.class, Context.class);
    log.trace("Creating TracingIterator spanName={}, delegate={}, connectorId={}", spanName,
        delegateIterator, connectorId);
  }

  @Override
  public boolean hasNext() {
    closeScopeAndEndSpan();
    return delegateIterator.hasNext();
  }

  /**
   * In addition to returning next traced record (if present) - creates a new span and capture
   * headers as span attributes.
   * <p>
   * Create a new span - named using SpanName set at wrapping.
   * <p>
   * Capture header key/values as Span attributes according to configured whitelist. Headers are
   * captured according to configuration as is (as byte[] values) and recorded to the span assuming
   * string values.
   *
   * @return next ConnectRecord in collection.
   */
  @Override
  public T next() {
    closeScopeAndEndSpan();
    T record = delegateIterator.next();
    recordSpan(record);
    return record;
  }

  protected void recordSpan(T record) {
    if (record != null) {
      Context parentContext = null;

      if (record instanceof SinkRecord) {
        parentContext = sinkRecordContextStore.get((SinkRecord) record);
        log.trace("SinkRecord={}, parentContext={}", record,
            parentContext != null ? parentContext : "not found");

      } else if (record instanceof SourceRecord) {
        parentContext = sourceRecordContextStore.get((SourceRecord) record);
        log.trace("SourceRecord={}, parentContext={}", record,
            parentContext != null ? parentContext : "not found");
      }
      if (parentContext == null) {
        parentContext = openTelemetryWrapper().currentContext();
      }

      String topicSpanName = String.format(SpanNames.TASK_SPAN_NAME_FORMAT, record.topic(),
          spanName);
      currentSpan = spanHandler().createAndStartSpan(topicSpanName, parentContext);
      currentScope = currentSpan.makeCurrent();
      spanHandler().captureServiceNameToCurrentSpan(connectorId);
      connectHandler().captureConnectHeadersToCurrentSpan(record.headers(),
          headerCaptureConfiguration().getHeaderValueEncoding());
      log.trace("Created Span in iterator.next, parentContext={}",
          parentContext);
    }
  }

  @Override
  public void remove() {
    delegateIterator.remove();
  }

  protected void closeScopeAndEndSpan() {
    if (currentScope != null) {
      currentSpan.end();
      currentScope.close();
      currentScope = null;
      currentSpan = null;
    }
  }
}
