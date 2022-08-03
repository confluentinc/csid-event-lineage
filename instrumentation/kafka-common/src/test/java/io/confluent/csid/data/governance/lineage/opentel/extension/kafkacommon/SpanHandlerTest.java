/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class SpanHandlerTest {

  OpenTelemetryWrapper mockOpenTelemetryWrapper = mock(OpenTelemetryWrapper.class);
  SpanHandler classUnderTest = new SpanHandler(mockOpenTelemetryWrapper,
      Constants.INSTRUMENTATION_NAME_KAFKA_STREAMS);

  @Test
  void createAndStartSpanWithLink() {
    String spanName = "test-span";
    Context mockContext = mock(Context.class);
    SpanContext linkedSpan = SpanContext.create("abc", "abcd", TraceFlags.getDefault(),
        TraceState.getDefault());
    SpanBuilder mockSpanBuilder = mock(SpanBuilder.class);
    Span span = Span.getInvalid();

    OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class, Answers.RETURNS_DEEP_STUBS);
    when(mockOpenTelemetryWrapper.globalOpenTelemetry()).thenReturn(mockOpenTelemetry);
    when(mockOpenTelemetry.getTracer(Constants.INSTRUMENTATION_NAME_KAFKA_STREAMS).spanBuilder(spanName)
        .setParent(mockContext)).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.startSpan()).thenReturn(span);

    Span returnedSpan = classUnderTest.createAndStartSpanWithLink(spanName, mockContext,
        linkedSpan);

    verify(mockSpanBuilder).addLink(linkedSpan);
    verify(mockSpanBuilder).startSpan();
    assertThat(returnedSpan).isEqualTo(span);
  }

  @Test
  void createAndStartSpan() {
    String spanName = "test-span";
    Context mockContext = mock(Context.class);
    Span span = Span.getInvalid();

    OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class, Answers.RETURNS_DEEP_STUBS);
    when(mockOpenTelemetryWrapper.globalOpenTelemetry()).thenReturn(mockOpenTelemetry);
    when(mockOpenTelemetry.getTracer(Constants.INSTRUMENTATION_NAME_KAFKA_STREAMS).spanBuilder(spanName)
        .setParent(mockContext).startSpan()).thenReturn(span);

    Span returnedSpan = classUnderTest.createAndStartSpan(spanName, mockContext);
    verify(mockOpenTelemetry.getTracer(Constants.INSTRUMENTATION_NAME_KAFKA_STREAMS).spanBuilder(spanName)
        .setParent(mockContext)).startSpan();
    assertThat(returnedSpan).isEqualTo(span);
  }

  @Test
  void addEventToSpan() {
    String eventName = "test-event";
    Attributes eventAttributes = Attributes.of(AttributeKey.stringKey("test-key"),
        "test-attribute");
    Span mockSpan = mock(Span.class);
    when(mockOpenTelemetryWrapper.currentSpan()).thenReturn(mockSpan);
    classUnderTest.addEventToSpan(mockSpan, eventName, eventAttributes);
    verify(mockSpan).addEvent(eventName, eventAttributes);
    verifyNoMoreInteractions(mockSpan);
  }
}