/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeaderCaptureConfiguration;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeadersHandler;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanHandler;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanSuppressionConfiguration;
import lombok.experimental.UtilityClass;

/**
 * Singleton instances used by Kafka Clients instrumentation extension
 */
@UtilityClass
public class Singletons {

  private static OpenTelemetryWrapper OPEN_TELEMETRY_WRAPPER = new OpenTelemetryWrapper();

  private static HeaderCaptureConfiguration HEADER_CAPTURE_CONFIGURATION = new HeaderCaptureConfiguration();

  private static HeadersHandler HEADERS_HANDLER = new HeadersHandler(openTelemetryWrapper(),
      headerCaptureConfiguration());

  private static final SpanHandler SPAN_HANDLER = new SpanHandler(openTelemetryWrapper(),
      Constants.INSTRUMENTATION_NAME_KAFKA_CLIENTS);

  private static final SpanSuppressionConfiguration SPAN_SUPPRESSION_CONFIGURATION = new SpanSuppressionConfiguration();

  private static final InterceptorHandler INTERCEPTOR_HANDLER = new InterceptorHandler(spanSuppressionConfiguration());

  public static OpenTelemetryWrapper openTelemetryWrapper() {
    return OPEN_TELEMETRY_WRAPPER;
  }

  public static HeadersHandler headersHandler() {
    return HEADERS_HANDLER;
  }

  public static HeaderCaptureConfiguration headerCaptureConfiguration() {
    return HEADER_CAPTURE_CONFIGURATION;
  }

  public static SpanHandler spanHandler() {
    return SPAN_HANDLER;
  }

  public static SpanSuppressionConfiguration spanSuppressionConfiguration() {
    return SPAN_SUPPRESSION_CONFIGURATION;
  }

  public static InterceptorHandler interceptorHandler() {
    return INTERCEPTOR_HANDLER;
  }

  /**
   * Specify helper classes for instrumentation to be loaded into agent class loader
   *
   * @param className to test for inclusion criteria
   * @return boolean specifying if the class is helper class or not.
   */
  public static boolean isHelperClass(String className) {
    return className.startsWith(
        "io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.")
        || className.startsWith(
        "io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.");
  }
}
