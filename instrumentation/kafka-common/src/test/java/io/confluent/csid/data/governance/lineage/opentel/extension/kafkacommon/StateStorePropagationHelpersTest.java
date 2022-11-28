/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.INT_SIZE_BYTES;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_MAGIC_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StateStorePropagationHelpersTest {

  OpenTelemetryWrapper mockOpenTelemetryWrapper = mock(OpenTelemetryWrapper.class);

  HeaderCaptureConfiguration mockHeaderCaptureConfiguration = mock(
      HeaderCaptureConfiguration.class);
  SpanHandler mockSpanHandler = mock(SpanHandler.class);
  HeadersHandler mockHeadersHandler = mock(HeadersHandler.class);

  StateStorePropagationHelpers classUnderTest = new StateStorePropagationHelpers(
      mockOpenTelemetryWrapper, mockSpanHandler, mockHeadersHandler,
      mockHeaderCaptureConfiguration);

  Charset charset = StandardCharsets.UTF_8;

  @BeforeEach
  void setup() {
    when(mockHeaderCaptureConfiguration.getHeaderValueEncoding()).thenReturn(charset);
  }

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
    HeadersHandler headersHandler = new HeadersHandler(mockOpenTelemetryWrapper,
        mockHeaderCaptureConfiguration);
    StateStorePropagationHelpers classUnderTest = new StateStorePropagationHelpers(
        mockOpenTelemetryWrapper, mockSpanHandler, headersHandler,
        mockHeaderCaptureConfiguration);

    byte[] value = new byte[]{'D', 'A', 'T', 'A'};
    byte[] traceId = "traceId".getBytes(StandardCharsets.UTF_8);
    HeaderSerde headerSerde = new HeaderSerde(new RecordHeader(TRACING_HEADER, traceId),
        StandardCharsets.UTF_8);

    ByteBuffer expected = ByteBuffer.allocate(
        TRACING_MAGIC_BYTES.length + INT_SIZE_BYTES + INT_SIZE_BYTES + headerSerde.length()
            + value.length);
    expected.put(TRACING_MAGIC_BYTES).putInt(value.length).putInt(1);
    headerSerde.put(expected);
    expected.put(value);

    assertThat(classUnderTest.attachTracingInformation(value, traceId, null)).isEqualTo(
        expected.array());
  }

  @Test
  void attachTracingInformationReturnsNullForNullValue() {
    byte[] value = null;
    byte[] traceId = "traceId".getBytes(StandardCharsets.UTF_8);
    assertThat(classUnderTest.attachTracingInformation(value, traceId, null)).isNull();
  }

  @Test
  void attachTracingInformationReturnsValueForNullTraceId() {
    byte[] value = new byte[]{'D', 'A', 'T', 'A'};
    byte[] traceId = null;
    assertThat(classUnderTest.attachTracingInformation(value, traceId, null)).isEqualTo(value);
  }

  @Test
  void handleStateStoreDeleteTrace() {
    HeadersHandler spyHeadersHandler = spy(new HeadersHandler(mockOpenTelemetryWrapper,
        mockHeaderCaptureConfiguration));
    StateStorePropagationHelpers classUnderTest = new StateStorePropagationHelpers(
        mockOpenTelemetryWrapper, mockSpanHandler, spyHeadersHandler,
        mockHeaderCaptureConfiguration);

    String storedTraceId = "TraceIdFromStateStore";
    String value = "test-value";
    String storeName = "testStore";
    Header[] headers = new Header[]{
        new RecordHeader("header1", "val1".getBytes(charset)),
        new RecordHeader("header2", "val2".getBytes(charset))
    };
    String expectedSpanName = String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT, storeName,
        SpanNames.STATE_STORE_DELETE);
    Span mockCurrentSpan = mock(Span.class);
    Span mockStateStoreDeleteSpan = mock(Span.class);
    Context mockExtractedContext = mock(Context.class);
    Context mockCurrentContext = mock(Context.class);
    SpanContext mockLinkedSpanContext = mock(SpanContext.class);
    Scope mockSpanScope = mock(Scope.class);

    byte[] valueAndTrace = classUnderTest.attachTracingInformation(
        value.getBytes(StandardCharsets.UTF_8), storedTraceId.getBytes(
            StandardCharsets.UTF_8), headers);

    when(mockOpenTelemetryWrapper.currentContext()).thenReturn(mockCurrentContext);
    when(mockOpenTelemetryWrapper.contextFromTraceIdString(storedTraceId)).thenReturn(
        mockExtractedContext);
    when(mockOpenTelemetryWrapper.currentSpan()).thenReturn(mockCurrentSpan);
    when(mockOpenTelemetryWrapper.spanContextFromContext(mockExtractedContext)).thenReturn(
        mockLinkedSpanContext);
    when(mockSpanHandler.createAndStartSpanWithLink(expectedSpanName, mockCurrentContext,
        mockLinkedSpanContext)).thenReturn(mockStateStoreDeleteSpan);
    when(mockStateStoreDeleteSpan.makeCurrent()).thenReturn(mockSpanScope);
    doNothing().when(mockSpanScope).close();

    classUnderTest.handleStateStoreDeleteTrace(storeName, valueAndTrace, false);
    verify(mockSpanHandler).addEventToSpan(mockCurrentSpan, expectedSpanName,
        Attributes.of(
            AttributeKey.stringKey(TRACING_HEADER), storedTraceId));
    verify(mockSpanHandler).createAndStartSpanWithLink(expectedSpanName,
        mockCurrentContext, mockLinkedSpanContext);

    verify(spyHeadersHandler).captureWhitelistedHeadersAsAttributesToCurrentSpan(headers);
    verify(mockSpanScope).close();
    verify(mockStateStoreDeleteSpan).end();
  }

  @Test
  void handleStateStoreGetTrace() {
    HeadersHandler spyHeadersHandler = spy(new HeadersHandler(mockOpenTelemetryWrapper,
        mockHeaderCaptureConfiguration));
    StateStorePropagationHelpers classUnderTest = new StateStorePropagationHelpers(
        mockOpenTelemetryWrapper, mockSpanHandler, spyHeadersHandler,
        mockHeaderCaptureConfiguration);

    String storedTraceId = "TraceIdFromStateStore";
    String value = "test-value";
    String storeName = "testStore";
    Header[] headers = new Header[]{
        new RecordHeader("header1", "val1".getBytes(charset)),
        new RecordHeader("header2", "val2".getBytes(charset))
    };
    Headers inScopeHeaders = new RecordHeaders();
    String expectedSpanName = String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT, storeName,
        SpanNames.STATE_STORE_GET);
    Span mockCurrentSpan = mock(Span.class);
    Span mockStateStoreGetSpan = mock(Span.class);
    Context mockExtractedContext = mock(Context.class);
    Context mockCurrentContext = mock(Context.class);
    SpanContext mockLinkedSpanContext = mock(SpanContext.class);
    ProcessorContext mockProcessorContext = mock(ProcessorContext.class);
    Scope mockSpanScope = mock(Scope.class);

    byte[] valueAndTrace = classUnderTest.attachTracingInformation(
        value.getBytes(StandardCharsets.UTF_8), storedTraceId.getBytes(
            StandardCharsets.UTF_8), headers);

    when(mockHeaderCaptureConfiguration.getHeaderPropagationWhitelist()).thenReturn(
        Arrays.stream(headers).map(
            Header::key).collect(
            Collectors.toSet()));

    when(mockProcessorContext.headers()).thenReturn(inScopeHeaders);
    when(mockOpenTelemetryWrapper.currentContext()).thenReturn(mockCurrentContext);
    when(mockOpenTelemetryWrapper.contextFromTraceIdString(storedTraceId)).thenReturn(
        mockExtractedContext);
    when(mockOpenTelemetryWrapper.currentSpan()).thenReturn(mockCurrentSpan);
    when(mockOpenTelemetryWrapper.spanContextFromContext(mockExtractedContext)).thenReturn(
        mockLinkedSpanContext);
    when(mockSpanHandler.createAndStartSpanWithLink(expectedSpanName, mockCurrentContext,
        mockLinkedSpanContext)).thenReturn(mockStateStoreGetSpan);
    when(mockStateStoreGetSpan.makeCurrent()).thenReturn(mockSpanScope);
    doNothing().when(mockSpanScope).close();

    classUnderTest.handleStateStoreGetTrace(storeName, valueAndTrace,
        mockProcessorContext.headers());
    verify(mockSpanHandler).addEventToSpan(mockCurrentSpan, expectedSpanName,
        Attributes.of(
            AttributeKey.stringKey(TRACING_HEADER), storedTraceId));
    verify(mockSpanHandler).createAndStartSpanWithLink(expectedSpanName,
        mockCurrentContext, mockLinkedSpanContext);

    verify(spyHeadersHandler).mergeHeaders(inScopeHeaders, headers);
    verify(spyHeadersHandler).captureWhitelistedHeadersAsAttributesToCurrentSpan(headers);
    verify(mockSpanScope).close();
    verify(mockStateStoreGetSpan).end();
  }

  @Test
  void handleStateStorePutTrace() {
    String currentTraceId = "currentTraceId";
    String value = "test-value";
    String storeName = "testStore";

    Header[] headers = new Header[]{
        new RecordHeader("header1", "val1".getBytes(charset)),
        new RecordHeader("header2", "val2".getBytes(charset))
    };
    String expectedSpanName = String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT, storeName,
        SpanNames.STATE_STORE_PUT);

    Span mockCurrentSpan = mock(Span.class);
    Span mockStateStorePutSpan = mock(Span.class);
    Context mockCurrentContext = mock(Context.class);
    Scope mockSpanScope = mock(Scope.class);

    when(mockOpenTelemetryWrapper.currentContext()).thenReturn(mockCurrentContext);
    when(mockOpenTelemetryWrapper.currentSpan()).thenReturn(mockCurrentSpan);
    when(mockOpenTelemetryWrapper.traceIdStringFromContext(mockCurrentContext)).thenReturn(
        currentTraceId);

    when(mockSpanHandler.createAndStartSpan(expectedSpanName, mockCurrentContext))
        .thenReturn(mockStateStorePutSpan);
    when(mockStateStorePutSpan.makeCurrent()).thenReturn(mockSpanScope);
    doNothing().when(mockSpanScope).close();

    classUnderTest.handleStateStorePutTrace(storeName, value.getBytes(StandardCharsets.UTF_8),
        headers, false);

    verify(mockSpanHandler).addEventToSpan(mockCurrentSpan, expectedSpanName,
        Attributes.of(
            AttributeKey.stringKey(TRACING_HEADER), currentTraceId));
    verify(mockSpanHandler).createAndStartSpan(expectedSpanName, mockCurrentContext);
    verify(mockHeadersHandler).captureWhitelistedHeadersAsAttributesToCurrentSpan(headers);
    verify(mockSpanScope).close();
    verify(mockStateStorePutSpan).end();
  }

  @Test
  void getOriginalDataLengthForValueAndTrace() {
    HeadersHandler headersHandler = new HeadersHandler(mockOpenTelemetryWrapper,
        mockHeaderCaptureConfiguration);
    StateStorePropagationHelpers classUnderTest = new StateStorePropagationHelpers(
        mockOpenTelemetryWrapper, mockSpanHandler, headersHandler,
        mockHeaderCaptureConfiguration);

    byte[] value = new byte[]{'D', 'A', 'T', 'A'};
    byte[] traceId = "traceId".getBytes(StandardCharsets.UTF_8);
    Header[] headers = new Header[]{
        new RecordHeader("header1", "val1".getBytes(charset)),
        new RecordHeader("header2", "val2".getBytes(charset))
    };
    byte[] valueWithTraceData = classUnderTest.attachTracingInformation(value, traceId, headers);

    assertThat(classUnderTest.getOriginalDataLength(valueWithTraceData, 0)).isEqualTo(value.length);
  }

  @Test
  void getOriginalDataLengthForValueAndTraceAndPrependedTimestamp() {
    HeadersHandler headersHandler = new HeadersHandler(mockOpenTelemetryWrapper,
        mockHeaderCaptureConfiguration);
    StateStorePropagationHelpers classUnderTest = new StateStorePropagationHelpers(
        mockOpenTelemetryWrapper, mockSpanHandler, headersHandler,
        mockHeaderCaptureConfiguration);

    byte[] value = new byte[]{'D', 'A', 'T', 'A'};
    byte[] traceId = "traceId".getBytes(StandardCharsets.UTF_8);
    Header[] headers = new Header[]{
        new RecordHeader("header1", "val1".getBytes(charset)),
        new RecordHeader("header2", "val2".getBytes(charset))
    };
    byte[] valueWithTraceData = classUnderTest.attachTracingInformation(value, traceId, headers);
    byte[] valueWithTraceDataAndTimestamp = ByteBuffer.allocate(valueWithTraceData.length + 8)
        .putLong(System.currentTimeMillis()).put(valueWithTraceData).array();

    assertThat(classUnderTest.getOriginalDataLength(valueWithTraceDataAndTimestamp, 8)).isEqualTo(
        value.length);
  }

  @Test
  void getOriginalDataLengthForValueWithoutTraceEqualsItself() {
    byte[] value = new byte[]{'D', 'A', 'T', 'A'};
    assertThat(classUnderTest.getOriginalDataLength(value, 0)).isEqualTo(value.length);
  }
}