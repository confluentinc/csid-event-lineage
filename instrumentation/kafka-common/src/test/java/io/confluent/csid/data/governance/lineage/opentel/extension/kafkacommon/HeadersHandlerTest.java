/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.HEADER_ATTRIBUTE_PREFIX;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.INT_SIZE_BYTES;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_MAGIC_BYTES;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.trace.Span;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

class HeadersHandlerTest {

  OpenTelemetryWrapper openTelemetryWrapperMock = mock(OpenTelemetryWrapper.class);
  HeaderCaptureConfiguration headerCaptureConfigurationMock = mock(
      HeaderCaptureConfiguration.class);
  HeadersHandler classUnderTest = new HeadersHandler(openTelemetryWrapperMock,
      headerCaptureConfigurationMock);

  @Test
  void filterHeadersFiltersHeadersIgnoringCase() {
    Set<String> whitelist = new HashSet<>() {{
      add("header1");
      add("header2");
    }};
    Header[] headersToFilter = new Header[]{
        new RecordHeader("header3", "v1".getBytes()),
        new RecordHeader("HeAdeR1", "v2".getBytes()),
        new RecordHeader("HEADER2", "v3".getBytes())
    };
    assertThat(classUnderTest.filterHeaders(headersToFilter, whitelist)).containsExactly(
        new RecordHeader("HeAdeR1", "v2".getBytes()),
        new RecordHeader("HEADER2", "v3".getBytes())
    );
  }

  @Test
  void filterHeadersFiltersMultipleHeadersWithSameKey() {
    Set<String> whitelist = new HashSet<>() {{
      add("header1");
      add("header2");
    }};
    Header[] headersToFilter = new Header[]{
        new RecordHeader("Header3", "v1".getBytes()),
        new RecordHeader("HeAdeR1", "v2".getBytes()),
        new RecordHeader("HEADER2", "v3".getBytes()),
        new RecordHeader("header4", "v4".getBytes()),
        new RecordHeader("header1", "v5".getBytes())
    };
    assertThat(classUnderTest.filterHeaders(headersToFilter, whitelist)).containsExactly(
        new RecordHeader("HeAdeR1", "v2".getBytes()),
        new RecordHeader("HEADER2", "v3".getBytes()),
        new RecordHeader("header1", "v5".getBytes())
    );
  }

  @Test
  void filterHeadersReturnsEmptyArrayForNullHeadersInput() {
    Set<String> whitelist = new HashSet<>() {{
      add("header1");
      add("header2");
    }};
    Header[] headersToFilter = null;
    assertThat(classUnderTest.filterHeaders(headersToFilter, whitelist)).isEqualTo(new Header[0]);
  }

  @Test
  void filterHeadersReturnsEmptyArrayForNullWhitelist() {
    Set<String> whitelist = null;
    Header[] headersToFilter = new Header[]{new RecordHeader("Header3", "v1".getBytes())};
    assertThat(classUnderTest.filterHeaders(headersToFilter, whitelist)).isEqualTo(new Header[0]);
  }

  @Test
  void filterHeadersReturnsEmptyArrayForNullEmptyWhitelist() {
    Set<String> whitelist = new HashSet<>();
    Header[] headersToFilter = new Header[]{new RecordHeader("Header3", "v1".getBytes())};
    assertThat(classUnderTest.filterHeaders(headersToFilter, whitelist)).isEqualTo(new Header[0]);
  }

  @Test
  void captureHeadersAsAttributesToCurrentSpanWithoutRepeatingHeaderKeys() {
    Set<String> whitelist = new HashSet<>() {{
      add("header1");
      add("header2");
    }};
    Header[] headers = new Header[]{
        new RecordHeader("header3", "v1".getBytes()),
        new RecordHeader("HeAdeR1", "v2".getBytes()),
        new RecordHeader("HEADER2", "v3".getBytes())
    };
    List<Header> expectedHeaders = Arrays.asList(
        new RecordHeader("HeAdeR1", "v2".getBytes()),
        new RecordHeader("HEADER2", "v3".getBytes())
    );
    Charset charset = StandardCharsets.UTF_8;
    Span spanMock = mock(Span.class);
    when(headerCaptureConfigurationMock.getHeaderCaptureWhitelist()).thenReturn(whitelist);
    when(headerCaptureConfigurationMock.getHeaderValueEncoding()).thenReturn(charset);
    when(openTelemetryWrapperMock.currentSpan()).thenReturn(spanMock);
    classUnderTest.captureWhitelistedHeadersAsAttributesToCurrentSpan(headers);
    expectedHeaders.forEach(header ->
        verify(spanMock).setAttribute(stringKey(HEADER_ATTRIBUTE_PREFIX + header.key()),
            new String(header.value(), charset))
    );
  }

  @Test
  void captureHeadersAsAttributesToCurrentSpanWithRepeatingHeaderKeysAddsIndexToAttributeName() {
    Set<String> whitelist = new HashSet<>() {{
      add("header1");
      add("header2");
    }};
    Header[] headers = new Header[]{
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("HeAdeR1", "v2".getBytes()),
        new RecordHeader("HEADER2", "v3".getBytes())
    };
    List<Header> expectedHeaders = Arrays.asList(
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("HeAdeR1", "v2".getBytes())
    );
    Charset charset = StandardCharsets.UTF_8;
    Span spanMock = mock(Span.class);
    when(headerCaptureConfigurationMock.getHeaderCaptureWhitelist()).thenReturn(whitelist);
    when(headerCaptureConfigurationMock.getHeaderValueEncoding()).thenReturn(charset);
    when(openTelemetryWrapperMock.currentSpan()).thenReturn(spanMock);
    classUnderTest.captureWhitelistedHeadersAsAttributesToCurrentSpan(headers);
    IntStream.range(0, expectedHeaders.size()).forEach(idx ->
        verify(spanMock).setAttribute(
            stringKey(HEADER_ATTRIBUTE_PREFIX + expectedHeaders.get(idx).key() + "." + idx),
            new String(expectedHeaders.get(idx).value(), charset))
    );
  }

  @Test
  void storeHeadersForPropagation() {
    Set<String> whitelist = new HashSet<>() {{
      add("header1");
      add("header2");
    }};

    Header[] headers = new Header[]{
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes()),
        new RecordHeader("HEADER3", "v3".getBytes())
    };

    when(headerCaptureConfigurationMock.getHeaderPropagationWhitelist()).thenReturn(whitelist);

    classUnderTest.storeHeadersForPropagation(headers);
    assertThat(HeadersHolder.get()).containsExactly(
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes()));
  }

  @Test
  void propagateStoredHeadersAddsHeadersToProducerRecord() {
    Header[] headers = new Header[]{
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes()),
    };
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", "test");

    classUnderTest.mergeHeaders(producerRecord.headers(), headers);
    assertThat(producerRecord.headers().toArray()).containsExactly(headers);
  }

  @Test
  void propagateStoredHeadersAddsHeadersToProducerRecordWhenMultipleHeadersWithSameKey() {
    Header[] headers = new Header[]{
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes()),
        new RecordHeader("HeAdeR1", "v3".getBytes()),

    };
    HeadersHolder.store(headers);
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", "test");

    classUnderTest.mergeHeaders(producerRecord.headers(), headers);
    assertThat(producerRecord.headers().toArray()).containsExactly(headers);
  }

  @Test
  void propagateStoredHeadersAddsHeadersToProducerRecordSkippingHeadersWithSameKeyIfPresentAlready() {
    Header[] headers = new Header[]{
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes()),
        new RecordHeader("header3", "skip".getBytes()),
        new RecordHeader("HeAdeR1", "v3".getBytes()),
    };

    Header[] expected = new Header[]{
        new RecordHeader("header3", "alreadyPresent".getBytes()),
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes()),
        new RecordHeader("HeAdeR1", "v3".getBytes()),
    };

    HeadersHolder.store(headers);
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", "test");
    producerRecord.headers().add("header3", "alreadyPresent".getBytes(StandardCharsets.UTF_8));

    classUnderTest.mergeHeaders(producerRecord.headers(), headers);
    assertThat(producerRecord.headers().toArray()).containsExactly(expected);
  }

  @Test
  void prependHeadersToValueBuildsByteArrayWithHeadersSerialized() {
    Charset charset = StandardCharsets.UTF_8;
    byte[] value = "SomeMessageValue".getBytes();
    Header tracingHeader = new RecordHeader(TRACING_HEADER, "traceid123".getBytes(charset));
    List<Header> headersToPrepend = Arrays.asList(
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes())
    );
    int numberOfHeaders = 3; // Tracing header + 2 propagated headers.
    List<byte[]> serializedHeaders = Stream.concat(Stream.of(tracingHeader),
            headersToPrepend.stream())
        .map(header -> serializeHeader(header, charset)).collect(
            Collectors.toList());
    int totalSize =
        TRACING_MAGIC_BYTES.length + INT_SIZE_BYTES + INT_SIZE_BYTES + serializedHeaders.stream()
            .mapToInt(bytes -> bytes.length).sum() + value.length;
    ByteBuffer buffer = ByteBuffer.allocate(totalSize).put(TRACING_MAGIC_BYTES)
        .putInt(value.length).putInt(numberOfHeaders);
    serializedHeaders.forEach(buffer::put);
    buffer.put(value);
    byte[] expectedPrependedValue = buffer.array();
    when(headerCaptureConfigurationMock.getHeaderValueEncoding()).thenReturn(charset);
    assertThat(
        classUnderTest.prependHeadersToValue(tracingHeader, headersToPrepend.toArray(Header[]::new),
            value)).isEqualTo(expectedPrependedValue);
  }

  @Test
  void prependHeadersToValueReturnsValueUnchangedIfNoTracingHeader() {
    Header[] headersToPrepend = new Header[]{
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes())
    };
    byte[] value = "SomeMessageValue".getBytes();

    assertThat(classUnderTest.prependHeadersToValue(null, headersToPrepend, value)).isEqualTo(
        value);
  }

  @Test
  void prependHeadersToValueReturnsNullForNullValue() {
    Header[] headersToPrepend = new Header[]{
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes())
    };
    Header tracingHeader = new RecordHeader(TRACING_HEADER, "traceid123".getBytes());

    assertThat(
        classUnderTest.prependHeadersToValue(tracingHeader, headersToPrepend, null)).isEqualTo(
        null);
  }


  @Test
  void extractHeaders() {
    Charset charset = StandardCharsets.UTF_8;
    byte[] value = "SomeMessageValue".getBytes();
    Header tracingHeader = new RecordHeader(TRACING_HEADER, "traceid123".getBytes(charset));
    List<Header> headersToPrepend = Arrays.asList(
        tracingHeader,
        new RecordHeader("HeAdeR1", "v1".getBytes()),
        new RecordHeader("header2", "v2".getBytes())
    );

    int numberOfHeaders = 3; // Tracing header + 2 propagated headers.
    List<byte[]> serializedHeaders = headersToPrepend.stream()
        .map(header -> serializeHeader(header, charset))
        .collect(Collectors.toList());
    int totalSize =
        TRACING_MAGIC_BYTES.length + INT_SIZE_BYTES + INT_SIZE_BYTES + serializedHeaders.stream()
            .mapToInt(bytes -> bytes.length).sum() + value.length;
    ByteBuffer buffer = ByteBuffer.allocate(totalSize).put(TRACING_MAGIC_BYTES)
        .putInt(value.length).putInt(numberOfHeaders);
    serializedHeaders.forEach(buffer::put);
    buffer.put(value);
    byte[] valueWithHeaders = buffer.array();
    when(headerCaptureConfigurationMock.getHeaderValueEncoding()).thenReturn(charset);

    Pair<List<Header>, byte[]> headersAndValue = classUnderTest.extractHeaders(valueWithHeaders);
    assertThat(headersAndValue.getLeft()).containsExactly(headersToPrepend.toArray(Header[]::new));
    assertThat(headersAndValue.getRight()).isEqualTo(value);
  }

  private byte[] serializeHeader(Header header, Charset charset) {
    byte[] keyBytes = header.key().getBytes(charset);
    byte[] valueBytes = header.value();
    int headerBytesLength =
        INT_SIZE_BYTES + keyBytes.length + INT_SIZE_BYTES + header.value().length;
    return ByteBuffer.allocate(headerBytesLength).putInt(keyBytes.length).put(keyBytes)
        .putInt(valueBytes.length).put(valueBytes).array();
  }
}