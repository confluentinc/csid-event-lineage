/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.ConsumerInterceptorInstrumentation;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanSuppressionConfiguration;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Interceptor instrumentation helper methods
 *
 * @see ConsumerInterceptorInstrumentation
 */
@RequiredArgsConstructor
@Slf4j
public class InterceptorHandler {

  private final SpanSuppressionConfiguration spanSuppressionConfiguration;

  /**
   * Determines if interceptor span creation should be suppressed
   * <p>
   * Uses {@link SpanSuppressionConfiguration#SPAN_SUPPRESSION_BLACKLIST_PROP} configuration to
   * determine if consume span creation in this Interceptor should be suppressed based on
   * Interceptor class name.
   * <p>
   * Note: Implementation is case-insensitive
   *
   * @param interceptorClassName Simple class name of the interceptor
   * @return boolean flag indicating whether interceptor span creation should be suppressed.
   */
  public boolean interceptorShouldBeSuppressed(String interceptorClassName) {
    Set<String> interceptorSuppressionBlacklist = spanSuppressionConfiguration.getInterceptorSuppressionBlacklist();
    log.debug("interceptorShouldBeSuppressed start, interceptorName={}, blacklist={}",
        interceptorClassName, interceptorSuppressionBlacklist);

    if (interceptorSuppressionBlacklist.isEmpty()) {
      return false;
    }

    if (interceptorSuppressionBlacklist.size() == 1
        && interceptorSuppressionBlacklist.contains("*")) {
      return true;
    }

    for (String blacklistedInterceptorName : interceptorSuppressionBlacklist) {
      if (blacklistedInterceptorName.equalsIgnoreCase(interceptorClassName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Suppresses consume span creation by setting {@link InterceptorSuppressionMarker} to true.
   */
  public void enableInterceptorSuppression() {
    InterceptorSuppressionMarker.setSuppression(true);
  }

  /**
   * Clears {@link InterceptorSuppressionMarker}
   */
  public void disableInterceptorSuppression() {
    InterceptorSuppressionMarker.clearSuppressionMarker();
  }
}
