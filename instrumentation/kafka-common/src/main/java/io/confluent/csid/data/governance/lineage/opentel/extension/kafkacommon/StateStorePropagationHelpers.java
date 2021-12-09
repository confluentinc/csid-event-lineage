/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.tuple.Pair;

public class StateStorePropagationHelpers {

  /**
   * Opentelemetry trace header name
   */
  public static final String TRACING_HEADER = "traceparent";
  /**
   * Magic byte used to store/get trace id to/from byte[] serialized value
   */
  public static final byte[] TRACING_MAGIC_BYTES = new byte[]{'T', 'R', 'A', 'C', 'E'};

  private final OpenTelemetryWrapper openTelemetryWrapper;

  private final PayloadHandler payloadHandler;

  private final SpanHandler spanHandler;

  public StateStorePropagationHelpers(OpenTelemetryWrapper openTelemetryWrapper,
      PayloadHandler payloadHandler, SpanHandler spanHandler) {
    this.openTelemetryWrapper = openTelemetryWrapper;
    this.payloadHandler = payloadHandler;
    this.spanHandler = spanHandler;
  }

  /**
   * Check if byte[] value contains TRACING_MAGIC_BYTES indicating presence of trace header.
   *
   * @param value to check
   * @return boolean indicating presence of TRACING_MAGIC_BYTES
   */
  public boolean hasTracingInfoAttached(byte[] value) {
    return value != null &&
        value.length > TRACING_MAGIC_BYTES.length &&
        value[0] == TRACING_MAGIC_BYTES[0] &&
        value[1] == TRACING_MAGIC_BYTES[1] &&
        value[2] == TRACING_MAGIC_BYTES[2] &&
        value[3] == TRACING_MAGIC_BYTES[3] &&
        value[4] == TRACING_MAGIC_BYTES[4];
  }

  /**
   * Prepends byte[] value with trace identifier for preservation in StateStore
   *
   * @param value           to prepend tracing information to
   * @param traceIdentifier to prepend onto value
   * @return byte[] containing combined value and trace identifier with magic bytes for later
   * detection.
   */
  public byte[] attachTracingInformation(byte[] value, byte[] traceIdentifier) {
    if (value == null || traceIdentifier == null) {
      return value;
    }
    int newPayloadSize = TRACING_MAGIC_BYTES.length + 4 + traceIdentifier.length + value.length;
    return ByteBuffer.allocate(newPayloadSize)
        .put(TRACING_MAGIC_BYTES)
        .putInt(traceIdentifier.length)
        .put(traceIdentifier).put(value)
        .array();
  }

  /**
   * Extracts tracing identifier from byte[] value retrieved from StateStore and strips tracing data
   * from value to restore original value byte[] for deserialization
   *
   * @param value byte[] value with prepended trace identifier as constructed by {@link
   *              #attachTracingInformation}
   * @return Pair of traceIdentifier String and originalValue byte[] (with trace bytes removed).
   */
  public Pair<String, byte[]> extractTracingInformation(byte[] value) {
    if (!hasTracingInfoAttached(value)) {
      throw new IllegalArgumentException("Value doesn't have tracing bytes attached");
    }
    int traceMagicLength = TRACING_MAGIC_BYTES.length;
    int intSize = 4;
    ByteBuffer byteBuffer = ByteBuffer.wrap(value, traceMagicLength,
        value.length - traceMagicLength);
    int traceIdLength = byteBuffer.getInt();
    byte[] traceIdBytes = new byte[traceIdLength];
    byteBuffer.get(traceIdBytes);

    int originalValueOffset = traceIdLength + traceMagicLength + intSize;
    int originalValueSize = value.length - originalValueOffset;
    byte[] originalValue = new byte[originalValueSize];
    byteBuffer.get(originalValue);
    return Pair.of(new String(traceIdBytes, StandardCharsets.UTF_8), originalValue);
  }

  /**
   * Record StateStore Get Span using given trace identifier (retrieved from StateStore), key and
   * value payloads.
   * <p>
   * Add Event to current span with storedTraceIdentifier
   * <p>
   * If executed with valid current span - create a new StateStore Get Span as a child of current
   * Span and a link to stored trace.
   * <p>
   * If executed with no valid current span - create a new StateStore Get Span as a
   * child/continuation of stored trace.
   * <p>
   * Record Key / Value payloads read from StateStore as attributes of the StateStore Get Span.
   *
   * @param storedTraceIdentifier trace identifier retrieved from StateStore
   * @param key                   payload key retrieved from StateStore - recorded as Span
   *                              attributes
   * @param value                 payload value retrieved from StateStore - recorded as Span
   *                              attributes
   */
  public void recordStateStoreGetSpanWithPayload(String storedTraceIdentifier, Object key,
      Object value) {

    spanHandler.addEventToSpan(openTelemetryWrapper.currentSpan(), SpanNames.STATE_STORE_GET,
        Attributes.of(AttributeKey.stringKey(TRACING_HEADER), storedTraceIdentifier));

    Context extractedContext = openTelemetryWrapper.contextFromTraceIdString(storedTraceIdentifier);

    Span stateStoreGetSpan;
    if (Span.getInvalid() == openTelemetryWrapper.currentSpan()) {
      stateStoreGetSpan = spanHandler.createAndStartSpan(SpanNames.STATE_STORE_GET,
          extractedContext);
    } else {
      SpanContext linkedSpanContext = openTelemetryWrapper.spanContextFromContext(extractedContext);
      stateStoreGetSpan = spanHandler.createAndStartSpanWithLink(SpanNames.STATE_STORE_GET,
          openTelemetryWrapper.currentContext(), linkedSpanContext);
    }
    payloadHandler.captureKeyValuePayloadsToSpan(key, value, stateStoreGetSpan);
    stateStoreGetSpan.end();
  }

  /**
   * Record StateStore Put Span, capturing key and value payloads.
   * <p>
   * Add Event to current span with trace identifier given - normally of current span.
   * <p>
   * Record Key / Value payloads stored to StateStore as attributes of the StateStore Put Span.
   *
   * @param traceIdentifier current trace identifier - recorded in Put event
   * @param key             payload key stored to StateStore - recorded as Span attributes
   * @param value           payload value stored to StateStore - recorded as Span attributes
   */
  public void recordStateStorePutSpanWithPayload(String traceIdentifier,
      Object key, Object value) {
    spanHandler.addEventToSpan(openTelemetryWrapper.currentSpan(), SpanNames.STATE_STORE_PUT,
        Attributes.of(AttributeKey.stringKey(TRACING_HEADER),
            traceIdentifier != null ? traceIdentifier : "null"));
    Span putSpan = spanHandler.createAndStartSpan(SpanNames.STATE_STORE_PUT,
        openTelemetryWrapper.currentContext());
    payloadHandler.captureKeyValuePayloadsToSpan(key, value, putSpan);
    putSpan.end();
  }
}
