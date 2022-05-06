/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeaderCaptureConfiguration;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeadersHandler;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
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

  public static OpenTelemetryWrapper openTelemetryWrapper() {
    return OPEN_TELEMETRY_WRAPPER;
  }

  public static HeadersHandler headersHandler() {
    return HEADERS_HANDLER;
  }

  public static HeaderCaptureConfiguration headerCaptureConfiguration() {
    return HEADER_CAPTURE_CONFIGURATION;
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
