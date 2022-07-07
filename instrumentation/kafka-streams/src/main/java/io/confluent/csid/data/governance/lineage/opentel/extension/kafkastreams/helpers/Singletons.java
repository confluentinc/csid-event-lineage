/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeaderCaptureConfiguration;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeadersHandler;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanHandler;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ValueAndTimestampHandler;
import lombok.experimental.UtilityClass;

/**
 * Singleton instances used by Kafka Streams instrumentation extension
 */
@UtilityClass
public class Singletons {

  private static final OpenTelemetryWrapper OPEN_TELEMETRY_WRAPPER = new OpenTelemetryWrapper();

  private static final HeaderCaptureConfiguration HEADER_CAPTURE_CONFIGURATION = new HeaderCaptureConfiguration();

  private static final HeadersHandler HEADERS_HANDLER = new HeadersHandler(openTelemetryWrapper(),
      headerCaptureConfiguration());

  private static final SpanHandler SPAN_HANDLER = new SpanHandler(openTelemetryWrapper(),
      Constants.INSTRUMENTATION_NAME_KAFKA_STREAMS);

  private static final StateStorePropagationHelpers STATE_STORE_PROPAGATION_HELPERS = new StateStorePropagationHelpers(
      openTelemetryWrapper(), spanHandler(), headersHandler(), headerCaptureConfiguration());

  public static final ValueAndTimestampHandler VALUE_AND_TIMESTAMP_HANDLER = new ValueAndTimestampHandler(
      stateStorePropagationHelpers());

  public static OpenTelemetryWrapper openTelemetryWrapper() {
    return OPEN_TELEMETRY_WRAPPER;
  }

  public static StateStorePropagationHelpers stateStorePropagationHelpers() {
    return STATE_STORE_PROPAGATION_HELPERS;
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

  public static ValueAndTimestampHandler valueAndTimestampHandler() {
    return VALUE_AND_TIMESTAMP_HANDLER;
  }

}
