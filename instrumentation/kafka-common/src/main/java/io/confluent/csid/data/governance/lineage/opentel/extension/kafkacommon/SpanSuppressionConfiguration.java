/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SpanSuppressionConfiguration {

  /**
   * Configuration property for span suppression blacklist - comma separated span names to be
   * excluded from trace / span capture.
   * <p>
   * Note: All spans starting with blacklisted name will be ignored - i.e. config of connect-status
   * will suppress spans like 'connect-status-update send' and 'connect-status process'
   * <p>
   * Typically, for Connect - following spans are internal (although some are configurable):
   * <pre>
   * "__consumer"
   * "_confluent"
   * "_confluent"
   * "connect-configs"
   * "connect-process"
   * "connect-status"
   * "connect-offsets"
   * </pre>
   * <p>
   * In addition, Rest endpoint spans can be disabled using Opentelemetry configuration
   * "-Dotel.instrumentation.common.experimental.suppress-controller-spans=true"
   */
  public static final String SPAN_SUPPRESSION_BLACKLIST_PROP = "event.lineage.span-suppression-list";

  /**
   * Configuration property for Consumer Interceptor suppression blacklist - comma separated
   * Consumer Interceptor class names to be excluded from trace / span capture.
   * <p>
   * Note: Case-insensitive, simple class names - exact match or * to suppress all consumer
   * interceptor tracing.
   */
  public static final String SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP = "event.lineage.span-suppression-consumer-interceptor-list";

  private Set<String> spanSuppressionBlacklist;
  private Set<String> interceptorSuppressionBlacklist;

  public SpanSuppressionConfiguration() {
    loadConfiguration();
  }

  private void loadConfiguration() {
    spanSuppressionBlacklist = new HashSet<>();
    interceptorSuppressionBlacklist = new HashSet<>();
    if (System.getProperties().containsKey(SPAN_SUPPRESSION_BLACKLIST_PROP)) {
      Arrays.stream(System.getProperty(SPAN_SUPPRESSION_BLACKLIST_PROP).split(","))
          .map(String::trim)
          .filter(prop -> prop.length() > 0)
          .forEach(prop -> spanSuppressionBlacklist.add(prop.toLowerCase()));
    }
    if (System.getProperties().containsKey(SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP)) {
      Arrays.stream(System.getProperty(SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP).split(","))
          .map(String::trim)
          .filter(prop -> prop.length() > 0)
          .forEach(prop -> interceptorSuppressionBlacklist.add(prop.toLowerCase()));
    }
  }


  /**
   * Configured span suppression blacklist.
   * <p>
   *
   * @return Set of span names to ignore
   * @see #SPAN_SUPPRESSION_BLACKLIST_PROP
   */
  public Set<String> getSpanSuppressionBlacklist() {
    return spanSuppressionBlacklist;
  }

  /**
   * Configured Consumer Interceptor suppression blacklist.
   * <p>
   *
   * @return Set of Consumer Interceptor class names to ignore tracing for
   * @see #SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP
   */
  public Set<String> getInterceptorSuppressionBlacklist() {
    return interceptorSuppressionBlacklist;
  }

  //Visible for testing - reload configuration in tests.
  void reloadConfiguration() {
    loadConfiguration();
  }
}


