/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.HEADER_ATTRIBUTE_PREFIX;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.INT_SIZE_BYTES;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_MAGIC_BYTES;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

import io.opentelemetry.api.trace.Span;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Handles header capture, propagation and merging / splitting of header and value byte arrays for
 * state store ops.
 */
@Slf4j
public class HeadersHandler {

  private final OpenTelemetryWrapper openTelemetryWrapper;
  private final HeaderCaptureConfiguration headerCaptureConfiguration;

  public HeadersHandler(OpenTelemetryWrapper openTelemetryWrapper,
      HeaderCaptureConfiguration headerCaptureConfiguration) {
    this.openTelemetryWrapper = openTelemetryWrapper;
    this.headerCaptureConfiguration = headerCaptureConfiguration;
  }

  /**
   * Filter headers against lower cased whitelist of header keys ignoring case.
   *
   * @param recordHeaders   headers array to filter
   * @param headerWhitelist Set of lower cased header keys to be used as whitelist for filtering
   * @return filtered array of headers
   */
  public Header[] filterHeaders(Header[] recordHeaders, Set<String> headerWhitelist) {
    if (recordHeaders == null || headerWhitelist == null || headerWhitelist.isEmpty()) {
      return new Header[0];
    }
    return Arrays.stream(recordHeaders).filter(x -> headerWhitelist.contains(x.key().toLowerCase()))
        .toArray(
            Header[]::new);
  }

  /**
   * Filters headers against configured header capture whitelist and captures header key/values as
   * current operation Span attributes prefixed with 'headers.'
   * <p>For example header HK1=val1 is captured as headers.HK1=val1 attribute.
   * <p>For repeating headers with same key - attribute keys are postfixed with index.
   * For example HK1=val1, HK1=val1other are captured as headers.HK1.1=val1,
   * headers.HK1.2=val1other
   *
   * @param headers headers to filter against whitelist and capture into the current Span.
   */
  public void captureWhitelistedHeadersAsAttributesToCurrentSpan(Header[] headers) {
    List<Header> headersToCaptureInSpan = Arrays.asList(filterHeaders(
        headers, headerCaptureConfiguration.getHeaderCaptureWhitelist()));
    if (headersToCaptureInSpan.isEmpty()) {
      return;
    }

    //convert list of headers to Map<key,List<value>> as multiple headers with same key can be present.
    //attributes have to have unique key so for multiple headers with same key - attribute name needs postfixed with index
    Map<String, List<String>> headerMap = new HashMap<>();
    headersToCaptureInSpan.forEach(header -> addHeaderToMap(header, headerMap));
    Span span = openTelemetryWrapper.currentSpan();
    headerMap.forEach((key, values) -> {
      if (values.size() == 1) { // single header with given key - record just as headers.key=value
        span.setAttribute(stringKey(HEADER_ATTRIBUTE_PREFIX + key), values.get(0));
      } else {   // multiple headers with given key - record as headers.key.0=value0, headers.key.1=value1 and so on
        IntStream.range(0, values.size()).forEach(idx ->
            span.setAttribute(stringKey(HEADER_ATTRIBUTE_PREFIX + key + "." + idx), values.get(idx))
        );
      }
    });
  }

  /**
   * Stores headers into ThreadLocal {@link HeadersHolder} for propagation later in Producer on
   * send.
   * <p>
   * See ExtendedKafkaProducerInstrumentation$ProducerExtensionAdvice in KafkaClients extension.
   *
   * @param headers to store
   */
  public void storeHeadersForPropagation(Headers headers) {
    HeadersHolder.store(headers);
  }

  /**
   * Adds headers to given Headers object but only if not already present.
   *
   * @param headersToMergeTo   destination Headers object to merge onto
   * @param headersToPropagate source headers array
   */
  public void mergeHeaders(Headers headersToMergeTo, Header[] headersToPropagate) {
    if (headersToPropagate == null || headersToPropagate.length == 0) {
      return;
    }

    Set<String> existingHeaderKeys = Arrays.stream(headersToMergeTo.toArray())
        .map(Header::key).collect(Collectors.toSet());
    List<Header> filteredHeadersToAddToRecord = Arrays.stream(headersToPropagate)
        .filter(header -> !existingHeaderKeys.contains(header.key()))
        .collect(Collectors.toList());
    filteredHeadersToAddToRecord.forEach(headersToMergeTo::add);
  }

  /**
   * Prepends headers to a byte array - headers are serialized and merged with byte[] of value for
   * preservation during storage in state store
   * <p>
   * Format - MagicBytes + value bytes length + no of headers + serialized headers + value bytes.
   *
   * @param tracingHeader - tracing header holding tracing information
   * @param headers       - additional headers to store (for capture in Spans and/or propagation)
   * @param originalValue - original byte serialized value
   * @return merged byte array as per format above.
   */
  public byte[] prependHeadersToValue(final Header tracingHeader, final Header[] headers,
      final byte[] originalValue) {
    if (originalValue == null || tracingHeader == null) {
      return originalValue;
    }

    //Merge tracing and propagation headers into list - making sure tracing is first header.
    List<Header> headersToAdd = new ArrayList<>();
    headersToAdd.add(tracingHeader);
    if (headers != null) {
      headersToAdd.addAll(Arrays.asList(headers));
    }

    // Calculate the required size
    // MAGIC + toBytes length + Nb of headers + [headers] + toBytes
    int requiredSize =
        TRACING_MAGIC_BYTES.length + INT_SIZE_BYTES + INT_SIZE_BYTES + originalValue.length;
    final List<HeaderSerde> serdes = new ArrayList<>();
    for (Header header : headersToAdd) {
      final HeaderSerde serde = new HeaderSerde(header,
          headerCaptureConfiguration.getHeaderValueEncoding());
      requiredSize += serde.length();
      serdes.add(serde);
    }

    // Fill the buffer
    final ByteBuffer headersBytes = ByteBuffer.allocate(requiredSize);

    // MAGIC
    headersBytes.put(TRACING_MAGIC_BYTES);
    // originalValue length
    headersBytes.putInt(originalValue.length);

    // Nb of Headers + [headers]
    headersBytes.putInt(serdes.size());
    serdes.forEach(value -> value.put(headersBytes));

    // originalValue
    headersBytes.put(originalValue);

    return headersBytes.array();
  }

  /**
   * Extracts stored headers(including trace header) from byte[] value retrieved from StateStore and
   * strips header data from value to restore original value byte[] for deserialization
   *
   * @param fromBytes byte[] value with prepended headers (including trace header) as constructed by
   *                  {@link #prependHeadersToValue(Header, Header[], byte[])}
   * @return Pair of List of stored headers (including tracing header)  and originalValue byte[]
   * (with trace bytes removed).
   * <p>
   * on exception - returns empty headers list and unmodified originalValue byte[]
   */
  public Pair<List<Header>, byte[]> extractHeaders(final byte[] fromBytes) {
    List<Header> headers = new ArrayList<>();

    try {
      final ByteBuffer buffer = ByteBuffer.wrap(fromBytes, TRACING_MAGIC_BYTES.length,
          fromBytes.length - TRACING_MAGIC_BYTES.length);

      final int originalValueSize = buffer.getInt();

      int nbOfHeaders = buffer.getInt();
      while (nbOfHeaders > 0) {
        final HeaderSerde serde = new HeaderSerde(buffer);

        headers.add(serde.toHeader(headerCaptureConfiguration.getHeaderValueEncoding()));

        nbOfHeaders--;
      }
      final byte[] bytes = new byte[originalValueSize];

      buffer.get(bytes);

      return Pair.of(headers, bytes);
    } catch (Throwable e) {
      log.warn("Error performing header extraction: ", e);
      return Pair.of(headers, fromBytes);
    }
  }


  private void addHeaderToMap(Header header, Map<String, List<String>> headerMap) {
    if (header.value() == null) {
      return;
    }
    String headerStringValue = new String(header.value(),
        headerCaptureConfiguration.getHeaderValueEncoding());
    if (headerMap.containsKey(header.key())) {
      headerMap.get(header.key()).add(headerStringValue);
    } else {
      headerMap.put(header.key(), new ArrayList<>() {{
        add(headerStringValue);
      }});
    }
  }

}
