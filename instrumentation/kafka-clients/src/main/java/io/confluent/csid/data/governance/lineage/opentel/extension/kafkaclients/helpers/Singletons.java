/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ObjectMapperUtil;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHandler;
import lombok.experimental.UtilityClass;

/**
 * Singleton instances used by Kafka Clients instrumentation extension
 */
@UtilityClass
public class Singletons {

  /**
   * ObjectMapper singleton for use in payload capture / Json mapping.
   */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final ObjectMapperUtil OBJECT_MAPPER_UTIL = new ObjectMapperUtil(objectMapper());

  private static final PayloadHandler PAYLOAD_HANDLER = new PayloadHandler(objectMapperUtil());

  private static final OpenTelemetryWrapper OPEN_TELEMETRY_WRAPPER = new OpenTelemetryWrapper();

  public static ObjectMapper objectMapper() {
    return OBJECT_MAPPER;
  }

  public static ObjectMapperUtil objectMapperUtil() {
    return OBJECT_MAPPER_UTIL;
  }

  public static PayloadHandler payloadHandler() {
    return PAYLOAD_HANDLER;
  }

  public static OpenTelemetryWrapper openTelemetryWrapper() {
    return OPEN_TELEMETRY_WRAPPER;
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
