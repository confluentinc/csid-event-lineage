/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;

public class OpenTelemetryWrapper {

  /**
   * Wraps access to static {@link GlobalOpenTelemetry#get()} call.
   *
   * @return Global {@link OpenTelemetry} object
   */
  public OpenTelemetry globalOpenTelemetry() {
    return GlobalOpenTelemetry.get();
  }

  /**
   * Wraps access to static {@link Span#current()} call
   *
   * @return Active {@link Span}
   */
  public Span currentSpan() {
    return Span.current();
  }

  /**
   * Wraps access to static {@link Context#current()} call
   *
   * @return Active {@link Context}
   */
  public Context currentContext() {
    return Context.current();
  }

  /**
   * Builds {@link SpanContext} from {@link Context} through {@link Span#fromContext(Context)} call
   *
   * @param context to build {@link SpanContext} from
   * @return {@link SpanContext} built from supplied {@link Context}
   */
  public SpanContext spanContextFromContext(Context context) {
    return Span.fromContext(context).getSpanContext();
  }

  /**
   * Builds {@link Context} from traceId supplied - usually after getting traceId from
   * transport/storage
   *
   * @param traceId to build {@link Context} from
   * @return {@link Context} built from supplied traceId String
   */
  public Context contextFromTraceIdString(String traceId) {
    return globalOpenTelemetry().getPropagators().getTextMapPropagator()
        .extract(currentContext(), traceId,
            StringTextMapGetter.getInstance());
  }

  /**
   * Builds traceId String from {@link Context} supplied for subsequent storage/propagation through
   * transport.
   *
   * @param context to build traceId String from
   * @return traceId String built from supplied {@link Context}
   */
  public String traceIdStringFromContext(Context context) {
    String[] traceIdentifierHolder = new String[1];
    globalOpenTelemetry().getPropagators().getTextMapPropagator()
        .inject(context, traceIdentifierHolder, StringTextMapSetter.getInstance());
    return traceIdentifierHolder[0];
  }
}
