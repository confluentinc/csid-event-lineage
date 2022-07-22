package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.util.List;

public class InheritedAttributesSpanProcessor implements SpanProcessor {

  private final List<AttributeKey<String>> inheritAttributeKeys;

  public InheritedAttributesSpanProcessor() {
    this.inheritAttributeKeys = List.of(Constants.SERVICE_NAME_KEY);
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
    ReadableSpan parentReadableSpan = (ReadableSpan) parentSpan;

    for (AttributeKey<String> inheritAttributeKey : inheritAttributeKeys) {
      String value = parentReadableSpan.getAttribute(inheritAttributeKey);
      if (value != null) {
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
