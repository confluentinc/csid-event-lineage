/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.util.List;

/**
 * Span attribute processor that copies configured attributes from parent span (if one exist in
 * current application context).
 * <p>
 * Useful for attribute propagation through span chain - for example SMT spans inherit service name
 * attribute from Sink / Source task spans using this propagation.
 */
public class InheritedAttributesSpanProcessor implements SpanProcessor {

  private final List<AttributeKey<String>> inheritAttributeKeys;
  private final List<AttributeKey<String>> inheritAttributeNoOverwriteKeys;

  public InheritedAttributesSpanProcessor(List<AttributeKey<String>> inheritAttributeKeys,
      List<AttributeKey<String>> inheritAttributeNoOverwriteKeys) {
    this.inheritAttributeKeys = inheritAttributeKeys;
    this.inheritAttributeNoOverwriteKeys = inheritAttributeNoOverwriteKeys;
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {
    Span parentSpan = Span.fromContextOrNull(parentContext);
    if (parentSpan == null) {
      return;
    }
    if (!(parentSpan instanceof ReadableSpan)) {
      return;
    }
    if (parentSpan.getSpanContext().isRemote()) {
      return;
    }

    ReadableSpan parentReadableSpan = (ReadableSpan) parentSpan;
    for (AttributeKey<String> inheritAttributeKey : inheritAttributeKeys) {
      String value = parentReadableSpan.getAttribute(inheritAttributeKey);
      if (value != null) {
        span.setAttribute(inheritAttributeKey, value);
      }
    }
    for (AttributeKey<String> inheritAttributeKey : inheritAttributeNoOverwriteKeys) {
      String value = parentReadableSpan.getAttribute(inheritAttributeKey);
      if (value != null && span.getAttribute(inheritAttributeKey) == null) {
        span.setAttribute(inheritAttributeKey, value);
      }
    }
  }

  @Override
  public boolean isStartRequired() {
    return true;
  }

  @Override
  public void onEnd(ReadableSpan span) {
  }

  @Override
  public boolean isEndRequired() {
    return false;
  }
}
