/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.connectHandler;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.headerCaptureConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.openTelemetryWrapper;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.spanHandler;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Iterator;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * Based on OpenTelemetry kafka-clients TracingIterator.
 * <p>
 * Creates a new Span and executes header capture logic on "next()" call.
 */
@Slf4j
public class TracingIterator<T extends ConnectRecord<T>>
    implements Iterator<T> {

  private final Iterator<T> delegateIterator;
  private final String spanName;
  private Scope currentScope;
  private Span currentSpan;

  /**
   * Wraps delegate iterator
   *
   * @param delegateIterator iterator to wrap
   * @param spanName         Span name for creating new spans on next() call.
   */
  public TracingIterator(
      Iterator<T> delegateIterator, String spanName) {
    this.spanName = spanName;
    this.delegateIterator = delegateIterator;
    log.trace("Creating TracingIterator spanName={}, delegate={}", spanName, delegateIterator);
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
      byte[] traceHeaderValue = Optional.ofNullable(record.headers()).flatMap(
          headers -> Optional.ofNullable(headers.lastWithName(
              Constants.TRACING_HEADER))).map(header -> connectHandler().convertHeaderValue(header,
          headerCaptureConfiguration().getHeaderValueEncoding())).orElse(null);
      Context parentContext;
      String traceId = null;
      if (traceHeaderValue != null) {
        traceId = new String(traceHeaderValue,
            headerCaptureConfiguration().getHeaderValueEncoding());
        parentContext = openTelemetryWrapper().contextFromTraceIdString(traceId);
      } else {
        parentContext = openTelemetryWrapper().currentContext();
      }
      String topicSpanName = String.format(SpanNames.TASK_SPAN_NAME_FORMAT, record.topic(),
          spanName);
      currentSpan = spanHandler().createAndStartSpan(topicSpanName, parentContext);
      currentScope = currentSpan.makeCurrent();
      connectHandler().captureConnectHeadersToCurrentSpan(record.headers(),
          headerCaptureConfiguration().getHeaderValueEncoding());
      log.trace("Created Span in iterator.next, parentTrace={}",
          traceId != null ? traceId : "null");
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
