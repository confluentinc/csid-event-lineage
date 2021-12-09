/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {

  public static final String INSTRUMENTATION_NAME = "io.opentelemetry.kafka-streams-2.6";

  public class SpanNames{
    public static final String STATE_STORE_GET = "state-store-get";
    public static final String STATE_STORE_PUT = "state-store-put";

  }
}
