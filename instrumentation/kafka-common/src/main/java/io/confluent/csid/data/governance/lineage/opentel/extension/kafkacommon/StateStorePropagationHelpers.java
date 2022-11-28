/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.INT_SIZE_BYTES;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_MAGIC_BYTES;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Handling of state store related operations
 */
public class StateStorePropagationHelpers {

  private final OpenTelemetryWrapper openTelemetryWrapper;
  private final HeadersHandler headersHandler;
  private final HeaderCaptureConfiguration headerCaptureConfiguration;

  private final SpanHandler spanHandler;

  public StateStorePropagationHelpers(OpenTelemetryWrapper openTelemetryWrapper,
      SpanHandler spanHandler, HeadersHandler headersHandler,
      HeaderCaptureConfiguration headerCaptureConfiguration) {
    this.openTelemetryWrapper = openTelemetryWrapper;
    this.spanHandler = spanHandler;
    this.headersHandler = headersHandler;
    this.headerCaptureConfiguration = headerCaptureConfiguration;
  }

  /**
   * Check if byte[] value contains TRACING_MAGIC_BYTES indicating presence of trace header.
   *
   * @param value to check
   * @return boolean indicating presence of TRACING_MAGIC_BYTES
   */
  public boolean hasTracingInfoAttached(byte[] value) {
    return hasTracingInfoAttachedAtOffset(value, 0);
  }

  /**
   * Check if byte[] value contains TRACING_MAGIC_BYTES indicating presence of trace header at
   * specified offset from start of byte array.
   *
   * @param value  to check
   * @param offset offset from start of byte[] - for cases when it's prepended with timestamp
   * @return boolean indicating presence of TRACING_MAGIC_BYTES
   */
  public boolean hasTracingInfoAttachedAtOffset(byte[] value, int offset) {
    return value != null &&
        value.length > offset + TRACING_MAGIC_BYTES.length &&
        value[offset] == TRACING_MAGIC_BYTES[0] &&
        value[offset + 1] == TRACING_MAGIC_BYTES[1] &&
        value[offset + 2] == TRACING_MAGIC_BYTES[2] &&
        value[offset + 3] == TRACING_MAGIC_BYTES[3] &&
        value[offset + 4] == TRACING_MAGIC_BYTES[4];
  }

  /**
   * Prepends byte[] value with trace identifier and headers for preservation in StateStore MAGIC +
   * value length + Nb of headers + [headers] + value
   *
   * @param value           to prepend tracing information to
   * @param traceIdentifier to prepend onto value
   * @param headers         headers to prepend onto value
   * @return byte[] containing combined value, trace identifier and headers with magic bytes for
   * later detection.
   */
  public byte[] attachTracingInformation(byte[] value, byte[] traceIdentifier, Header[] headers) {
    if (value == null || traceIdentifier == null) {
      return value;
    }
    return headersHandler.prependHeadersToValue(new RecordHeader(TRACING_HEADER, traceIdentifier),
        headers, value);
  }


  /**
   * Extracts original data length from byte[] containing value and trace data merged. Original
   * value length is stored after magic bytes. Optionally read at offset for situations when
   * timestamp is prepended to the byte array.
   *
   * @param value  byte[] value with prepended trace data as constructed by
   *               {@link #attachTracingInformation}
   * @param offset offset from start of byte[] - when trace data and value are prepended with
   *               timestamp
   * @return length of original value.
   */
  public int getOriginalDataLength(byte[] value, int offset) {
    if (!hasTracingInfoAttachedAtOffset(value, offset)) {
      return value.length;
    }
    ByteBuffer byteBuffer = ByteBuffer.wrap(value, TRACING_MAGIC_BYTES.length + offset,
        INT_SIZE_BYTES);
    return byteBuffer.getInt();
  }


  /**
   * Record StateStore Delete Span on Session Remove operation using current in scope span and
   * headers
   *
   * @param stateStoreName   state store name to use for Span naming
   * @param headersToCapture headers to capture into Span as attributes
   */
  public void handleStateStoreSessionRemoveSpan(String stateStoreName, Header[] headersToCapture, boolean isCache) {
    String spanOp = isCache ? SpanNames.STATE_STORE_CACHE_REMOVE : SpanNames.STATE_STORE_REMOVE;

    String spanName = String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT, stateStoreName,
        spanOp);

    Span currentInScope = openTelemetryWrapper.currentSpan();
    Span deleteSpan = spanHandler.createAndStartSpan(spanName,
        openTelemetryWrapper.currentContext());
    Scope deleteScope = deleteSpan.makeCurrent();

    headersHandler.captureWhitelistedHeadersAsAttributesToCurrentSpan(headersToCapture);

    deleteSpan.end();
    deleteScope.close();
    currentInScope.makeCurrent();
  }

  /**
   * Handles StateStore delete operation
   * <p>
   * Extracts trace and header data from value byte[]
   * <p>
   * Adds Event to current span with storedTraceIdentifier
   * <p>
   * If executed with valid current span - create a new StateStore Delete Span as a child of current
   * Span and a link to stored trace of the deleted value.
   * <p>
   * If executed with no valid current span - create a new StateStore Delete Span as a
   * child/continuation of stored trace.
   * <p>
   * Records Key / Value of headers read from StateStore as attributes of the StateStore Delete
   * Span.
   * <p>
   * If value byte[] doesn't contain trace data - records span as child of current with attribute
   * indicating delete of non-traced value
   *
   * @param stateStoreName state store name to use for Span naming
   * @param bytesValue     byte[] of the value (normally with merged tracing / header data) being
   *                       deleted
   * @return raw value bytes with tracing data removed.
   */
  public byte[] handleStateStoreDeleteTrace(String stateStoreName, byte[] bytesValue, boolean isCache) {

    if (!hasTracingInfoAttached(bytesValue)) {
      Span deleteSpan = spanHandler.createAndStartSpan(
          String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT, stateStoreName,
              SpanNames.STATE_STORE_DELETE), openTelemetryWrapper.currentContext());
      deleteSpan.setAttribute("Warning", "Deleted value does not contain Tracing information");
      deleteSpan.end();
      return bytesValue;
    }

    Pair<List<Header>, byte[]> headersAndStrippedValue = headersHandler.extractHeaders(bytesValue);
    List<Header> extractedHeaders = headersAndStrippedValue.getLeft();
    Header tracingHeader = extractedHeaders.stream()
        .filter(header -> header.key().equals(TRACING_HEADER)).findFirst().orElse(null);
    extractedHeaders.remove(tracingHeader);
    Header[] allOtherHeaders = extractedHeaders.toArray(Header[]::new);

    String traceIdentifier = tracingHeader != null ? new String(tracingHeader.value(),
        headerCaptureConfiguration.getHeaderValueEncoding()) : null;
    bytesValue = headersAndStrippedValue.getRight();

    recordStateStoreDeleteSpan(stateStoreName, traceIdentifier,
        allOtherHeaders, isCache);

    return bytesValue;
  }

  /**
   * Handles StateStore Get/Fetch operations
   * <p>
   * Extracts trace and header data from value byte[] and strips them off restoring original value
   * byte[] ready for deserialization
   * <p>
   * Adds Event to current span with storedTraceIdentifier
   * <p>
   * If executed with valid current span - creates a new StateStore Get Span as a child of current
   * Span and a link to stored trace.
   * <p>
   * If executed with no valid current span - creates a new StateStore Get Span as a
   * child/continuation of stored trace.
   * <p>
   * Records Key / Value of headers read from StateStore as attributes of the StateStore Get Span.
   * <p>
   * Propagates extracted headers onto current processor context according to configured whitelist
   * {@link HeaderCaptureConfiguration#getHeaderPropagationWhitelist()}
   *
   * @param stateStoreName         state store name to use for Span naming
   * @param bytesValue             byte[] of the value (normally with merged tracing / header data)
   *                               being retrieved from state store - trace data is stripped from it
   *                               during trace/header data extraction
   * @param headersToPropagateOnto current processor context headers to propagate/merge whitelisted
   *                               extracted headers onto
   * @return raw value bytes with tracing data removed.
   */
  public byte[] handleStateStoreGetTrace(String stateStoreName, byte[] bytesValue,
      Headers headersToPropagateOnto) {

    if (!hasTracingInfoAttached(bytesValue)) {
      return bytesValue;
    }

    Pair<List<Header>, byte[]> headersAndStrippedValue = headersHandler.extractHeaders(bytesValue);
    List<Header> extractedHeaders = headersAndStrippedValue.getLeft();
    Header tracingHeader = extractedHeaders.stream()
        .filter(header -> header.key().equals(TRACING_HEADER)).findFirst().orElse(null);
    extractedHeaders.remove(tracingHeader);
    Header[] allOtherHeaders = extractedHeaders.toArray(Header[]::new);

    String traceIdentifier = tracingHeader != null ? new String(tracingHeader.value(),
        headerCaptureConfiguration.getHeaderValueEncoding()) : null;
    bytesValue = headersAndStrippedValue.getRight();

    Header[] headersToPropagate = headersHandler.filterHeaders(allOtherHeaders,
        headerCaptureConfiguration.getHeaderPropagationWhitelist());
    headersHandler.mergeHeaders(headersToPropagateOnto, headersToPropagate);
    recordStateStoreGetSpanWithHeaders(stateStoreName, traceIdentifier,
        allOtherHeaders);

    return bytesValue;
  }

  /**
   * Handles StateStore Put operations
   * <p>
   * Prepends value byte[] with trace and header data from current tracing context and processor
   * context
   * <p>
   * Adds Event to current span with trace identifier that is appended to byte[] value
   * <p>
   * Creates a new StateStore Put Span as a child of current Span
   * <p>
   * Records Key / Value of whitelisted headers as attributes of the StateStore Put Span.
   * <p>
   *
   * @param stateStoreName state store name to use for Span naming
   * @param value          byte[] of the value being put into state store
   * @param headers        current processor context headers to filter according to configured
   *                       whitelists, prepend to value and capture as Span attributes
   * @return byte array consisting of trace, header information and value
   */
  public byte[] handleStateStorePutTrace(String stateStoreName, byte[] value, Header[] headers,
      boolean isCache) {
    if (hasTracingInfoAttached(value)) {
      return value;
    }

    String traceIdentifier = openTelemetryWrapper
        .traceIdStringFromContext(openTelemetryWrapper.currentContext());

    Header[] headersForPropagation = new Header[0];

    if (headers != null && headers.length > 0) {
      headersForPropagation = headersHandler.filterHeaders(headers,
          headerCaptureConfiguration.getHeaderPropagationWhitelist());
    }

    if (traceIdentifier != null) {
      value = attachTracingInformation(value,
          traceIdentifier.getBytes(StandardCharsets.UTF_8), headersForPropagation);
    }

    recordStateStorePutSpanWithHeaders(stateStoreName, traceIdentifier, headers, isCache);
    return value;
  }


  private void recordStateStoreGetSpanWithHeaders(String stateStoreName,
      String storedTraceIdentifier, Header[] headersToCapture) {

    String spanName = String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT, stateStoreName,
        SpanNames.STATE_STORE_GET);

    if (storedTraceIdentifier == null) {
      spanHandler.addEventToSpan(openTelemetryWrapper.currentSpan(), spanName,
          Attributes.of(AttributeKey.stringKey(TRACING_HEADER),
              "Failed to extract tracing header from stateStore value!"));
      return;
    }

    spanHandler.addEventToSpan(openTelemetryWrapper.currentSpan(), spanName,
        Attributes.of(AttributeKey.stringKey(TRACING_HEADER), storedTraceIdentifier));

    Context extractedContext = openTelemetryWrapper.contextFromTraceIdString(storedTraceIdentifier);
    Span currentInScope = openTelemetryWrapper.currentSpan();
    Span stateStoreGetSpan;

    if (Span.getInvalid() == openTelemetryWrapper.currentSpan()) {
      stateStoreGetSpan = spanHandler.createAndStartSpan(spanName, extractedContext);
    } else {
      SpanContext linkedSpanContext = openTelemetryWrapper.spanContextFromContext(extractedContext);
      stateStoreGetSpan = spanHandler.createAndStartSpanWithLink(spanName,
          openTelemetryWrapper.currentContext(), linkedSpanContext);
    }
    Scope getScope = stateStoreGetSpan.makeCurrent();
    headersHandler.captureWhitelistedHeadersAsAttributesToCurrentSpan(headersToCapture);
    stateStoreGetSpan.end();
    getScope.close();
    currentInScope.makeCurrent();
  }

  private void recordStateStorePutSpanWithHeaders(String stateStoreName, String traceIdentifier,
      Header[] headers, boolean isCache) {
    String spanOp = isCache ? SpanNames.STATE_STORE_CACHE_PUT : SpanNames.STATE_STORE_PUT;
    String spanName = String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT, stateStoreName,
        spanOp);
    spanHandler.addEventToSpan(openTelemetryWrapper.currentSpan(), spanName,
        Attributes.of(AttributeKey.stringKey(TRACING_HEADER),
            traceIdentifier != null ? traceIdentifier : "null"));
    Span currentInScope = openTelemetryWrapper.currentSpan();
    Span putSpan = spanHandler.createAndStartSpan(spanName, openTelemetryWrapper.currentContext());
    Scope putScope = putSpan.makeCurrent();
    headersHandler.captureWhitelistedHeadersAsAttributesToCurrentSpan(headers);
    putSpan.end();
    putScope.close();
    currentInScope.makeCurrent();
  }


  private void recordStateStoreDeleteSpan(String stateStoreName,
      String storedTraceIdentifier, Header[] headersToCapture, boolean isCache) {
    String spanOp = isCache? SpanNames.STATE_STORE_CACHE_DELETE : SpanNames.STATE_STORE_DELETE;
    String spanName = String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT, stateStoreName,
        SpanNames.STATE_STORE_DELETE);

    if (storedTraceIdentifier == null) {
      spanHandler.addEventToSpan(openTelemetryWrapper.currentSpan(), spanName,
          Attributes.of(AttributeKey.stringKey(TRACING_HEADER),
              "Failed to extract tracing header from stateStore value!"));
      return;
    }

    spanHandler.addEventToSpan(openTelemetryWrapper.currentSpan(), spanName,
        Attributes.of(AttributeKey.stringKey(TRACING_HEADER), storedTraceIdentifier));

    Context extractedContext = openTelemetryWrapper.contextFromTraceIdString(storedTraceIdentifier);

    Span currentInScope = openTelemetryWrapper.currentSpan();
    Span deleteSpan;

    if (Span.getInvalid() == openTelemetryWrapper.currentSpan()) {
      deleteSpan = spanHandler.createAndStartSpan(spanName, extractedContext);
    } else {
      SpanContext linkedSpanContext = openTelemetryWrapper.spanContextFromContext(extractedContext);
      deleteSpan = spanHandler.createAndStartSpanWithLink(spanName,
          openTelemetryWrapper.currentContext(), linkedSpanContext);
    }
    Scope deleteScope = deleteSpan.makeCurrent();

    headersHandler.captureWhitelistedHeadersAsAttributesToCurrentSpan(headersToCapture);

    deleteSpan.end();
    deleteScope.close();
    currentInScope.makeCurrent();
  }

  public Pair<Span, Scope> handleStateStoreFlushTrace(String stateStoreName, byte[] bytesValue,
      Headers headersToPropagateOnto, Context parentContext) {
    if (bytesValue == null) {
      return Pair.of(null, null);
    }
    Span flushSpan = spanHandler.createAndStartSpan(
        String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT, stateStoreName,
            SpanNames.STATE_STORE_FLUSH), parentContext);
    Scope flushScope = flushSpan.makeCurrent();

    if (hasTracingInfoAttached(bytesValue)) {
      Pair<List<Header>, byte[]> headersAndStrippedValue = headersHandler.extractHeaders(
          bytesValue);
      List<Header> extractedHeaders = headersAndStrippedValue.getLeft();
      Header[] allOtherHeaders = extractedHeaders.stream()
          .filter(header -> !header.key().equals(TRACING_HEADER)).toArray(Header[]::new);

      Header[] headersToPropagate = headersHandler.filterHeaders(allOtherHeaders,
          headerCaptureConfiguration.getHeaderPropagationWhitelist());
      RecordHeaders headers = new RecordHeaders(HeadersHolder.get());
      headersHandler.mergeHeaders(headers, headersToPropagate);
      HeadersHolder.store(headers);
      headersHandler.mergeHeaders(headersToPropagateOnto, headersToPropagate);
      headersHandler.captureWhitelistedHeadersAsAttributesToCurrentSpan(allOtherHeaders);
    }
    return Pair.of(flushSpan, flushScope);
  }

  public byte[] restoreRawValue(byte[] value) {
    byte[] restored = value;
    if (hasTracingInfoAttached(value)) {
      restored = headersHandler.extractHeaders(value).getRight();
    }
    return restored;
  }
}
