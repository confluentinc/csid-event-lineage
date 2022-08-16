/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.ConsumerInterceptorInstrumentation;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanSuppressionConfiguration;
import lombok.Value;

/**
 * ThreadLocal interceptor suppression marker - set depending on
 * {@link SpanSuppressionConfiguration#SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP} and interceptor
 * name.
 *
 * @see ConsumerInterceptorInstrumentation
 * @see InterceptorHandler
 */
@Value
public class InterceptorSuppressionMarker {

  private static final ThreadLocal<Boolean> MARKER = new ThreadLocal<>();

  public static void setSuppression(boolean suppressionFlag) {
    MARKER.set(suppressionFlag);
  }

  public static void clearSuppressionMarker() {
    MARKER.remove();
  }

  public static Boolean isSuppressionEnabled() {
    return MARKER.get() != null && MARKER.get();
  }
}
