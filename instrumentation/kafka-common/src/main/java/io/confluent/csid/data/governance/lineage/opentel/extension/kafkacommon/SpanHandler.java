/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;

public class SpanHandler {

  private final OpenTelemetryWrapper openTelemetryWrapper;
  private final String instrumentationName;

  public SpanHandler(OpenTelemetryWrapper openTelemetryWrapper, String instrumentationName) {
    this.openTelemetryWrapper = openTelemetryWrapper;
    this.instrumentationName = instrumentationName;
  }

  public Span createAndStartSpanWithLink(String spanName, Context parent, SpanContext linkedSpan) {
    return createAndStartSpan(spanName, parent, linkedSpan);
  }

  public Span createAndStartSpan(String spanName, Context parent) {
    return createAndStartSpan(spanName, parent, null);
  }

  public void addEventToSpan(Span span, String eventName, Attributes eventAttributes) {
    span.addEvent(eventName, eventAttributes);
  }

  private Span createAndStartSpan(String spanName, Context parent, SpanContext linkedSpan) {
    SpanBuilder spanBuilder = openTelemetryWrapper.globalOpenTelemetry()
        .getTracer(instrumentationName)
        .spanBuilder(spanName)
        .setParent(parent);
    if (null != linkedSpan) {
      spanBuilder.addLink(linkedSpan);
    }
    return spanBuilder.startSpan();
  }
}
