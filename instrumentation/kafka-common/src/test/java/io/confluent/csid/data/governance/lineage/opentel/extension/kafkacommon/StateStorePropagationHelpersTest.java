/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers.TRACING_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers.TRACING_MAGIC_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

class StateStorePropagationHelpersTest {

  OpenTelemetryWrapper mockOpenTelemetryWrapper = mock(OpenTelemetryWrapper.class);
  PayloadHandler mockPayloadHandler = mock(PayloadHandler.class);
  SpanHandler mockSpanHandler = mock(SpanHandler.class);
  StateStorePropagationHelpers classUnderTest = new StateStorePropagationHelpers(
      mockOpenTelemetryWrapper, mockPayloadHandler, mockSpanHandler);

  @Test
  void hasTracingInfoAttachedTrueWhenValueStartsWithMagicBytes() {
    byte[] value = new byte[]{'T', 'R', 'A', 'C', 'E', 'A', 'N', 'D', 'D', 'A', 'T', 'A'};
    assertThat(classUnderTest.hasTracingInfoAttached(value)).isTrue();
  }

  @Test
  void hasTracingInfoAttachedFalseWhenValueDoesntStartWithMagicBytes() {
    byte[] value = new byte[]{'A', 'B', 'A', 'C', 'E'};
    assertThat(classUnderTest.hasTracingInfoAttached(value)).isFalse();
  }

  @Test
  void hasTracingInfoAttachedFalseWhenValueIsNull() {
    byte[] value = null;
    assertThat(classUnderTest.hasTracingInfoAttached(value)).isFalse();
  }

  @Test
  void attachTracingInformationAddsMagicBytesTraceIdAndLengthToValue() {
    byte[] value = new byte[]{'D', 'A', 'T', 'A'};
    byte[] traceId = "traceId".getBytes(StandardCharsets.UTF_8);
    byte[] traceIdLength = ByteBuffer.allocate(4).putInt(traceId.length).array();
    byte[] valueWithTrace = ArrayUtils.addAll(TRACING_MAGIC_BYTES, traceIdLength);
    valueWithTrace = ArrayUtils.addAll(valueWithTrace, traceId);
    valueWithTrace = ArrayUtils.addAll(valueWithTrace, value);

    assertThat(classUnderTest.attachTracingInformation(value, traceId)).isEqualTo(valueWithTrace);
  }

  @Test
  void attachTracingInformationReturnsNullForNullValue() {
    byte[] value = null;
    byte[] traceId = "traceId".getBytes(StandardCharsets.UTF_8);
    assertThat(classUnderTest.attachTracingInformation(value, traceId)).isNull();
  }

  @Test
  void attachTracingInformationReturnsValueForNullTraceId() {
    byte[] value = new byte[]{'D', 'A', 'T', 'A'};
    byte[] traceId = null;
    assertThat(classUnderTest.attachTracingInformation(value, traceId)).isEqualTo(value);
  }

  @Test
  void extractTracingInformationReturnsTraceIdAndOriginalValue() {
    byte[] originalValue = new byte[]{'D', 'A', 'T', 'A'};
    byte[] traceId = "traceId".getBytes(StandardCharsets.UTF_8);

    byte[] value = classUnderTest.attachTracingInformation(originalValue, traceId);

    Pair<String, byte[]> traceAndStrippedValue = classUnderTest.extractTracingInformation(value);
    assertThat(traceAndStrippedValue.getLeft()).isEqualTo(
        new String(traceId, StandardCharsets.UTF_8));
    assertThat(traceAndStrippedValue.getRight()).isEqualTo(originalValue);
  }

  @Test
  void extractTracingInformationThrowsExceptionForValueWithoutTracingBytes() {
    byte[] value = new byte[]{'D', 'A', 'T', 'A'};
    assertThrows(IllegalArgumentException.class,
        () -> classUnderTest.extractTracingInformation(value));
  }

  @Test
  void recordStateStoreGetSpanWithPayload() {
    String storedTraceId = "TraceIdFromStateStore";
    String key = "test-key";
    String value = "test-value";
    Span mockCurrentSpan = mock(Span.class);
    Span mockStateStoreGetSpan = mock(Span.class);
    Context mockExtractedContext = mock(Context.class);
    Context mockCurrentContext = mock(Context.class);
    SpanContext mockLinkedSpanContext = mock(SpanContext.class);

    when(mockOpenTelemetryWrapper.currentContext()).thenReturn(mockCurrentContext);
    when(mockOpenTelemetryWrapper.contextFromTraceIdString(storedTraceId)).thenReturn(
        mockExtractedContext);
    when(mockOpenTelemetryWrapper.currentSpan()).thenReturn(mockCurrentSpan);
    when(mockOpenTelemetryWrapper.spanContextFromContext(mockExtractedContext)).thenReturn(
        mockLinkedSpanContext);
    when(mockSpanHandler.createAndStartSpanWithLink(SpanNames.STATE_STORE_GET, mockCurrentContext,
        mockLinkedSpanContext)).thenReturn(mockStateStoreGetSpan);

    classUnderTest.recordStateStoreGetSpanWithPayload(storedTraceId, key, value);
    verify(mockSpanHandler).addEventToSpan(mockCurrentSpan, SpanNames.STATE_STORE_GET,
        Attributes.of(
            AttributeKey.stringKey(TRACING_HEADER), storedTraceId));
    verify(mockSpanHandler).createAndStartSpanWithLink(SpanNames.STATE_STORE_GET,
        mockCurrentContext, mockLinkedSpanContext);
    verify(mockPayloadHandler).captureKeyValuePayloadsToSpan(key, value, mockStateStoreGetSpan);
    verify(mockStateStoreGetSpan).end();
  }

  @Test
  void recordStateStorePutSpanWithPayload() {
    String currentTraceId = "currentTraceId";
    String key = "test-key";
    String value = "test-value";
    Span mockCurrentSpan = mock(Span.class);
    Span mockStateStorePutSpan = mock(Span.class);
    Context mockCurrentContext = mock(Context.class);

    when(mockOpenTelemetryWrapper.currentContext()).thenReturn(mockCurrentContext);
    when(mockOpenTelemetryWrapper.currentSpan()).thenReturn(mockCurrentSpan);

    when(mockSpanHandler.createAndStartSpan(SpanNames.STATE_STORE_PUT, mockCurrentContext))
        .thenReturn(mockStateStorePutSpan);

    classUnderTest.recordStateStorePutSpanWithPayload(currentTraceId, key, value);

    verify(mockSpanHandler).addEventToSpan(mockCurrentSpan, SpanNames.STATE_STORE_PUT,
        Attributes.of(
            AttributeKey.stringKey(TRACING_HEADER), currentTraceId));
    verify(mockSpanHandler).createAndStartSpan(SpanNames.STATE_STORE_PUT,
        mockCurrentContext);
    verify(mockPayloadHandler).captureKeyValuePayloadsToSpan(key, value, mockStateStorePutSpan);
    verify(mockStateStorePutSpan).end();
  }

}