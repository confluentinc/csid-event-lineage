/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TestConstants {

  /**
   * Tests tagged with this tag are not propagating Tracing Context through Kafka message headers.
   * That way consumption of non-traced messages can be simulated - for scenarios where for example
   * Connect Replicator has tracing installed, but producing application does not.
   */
  public static final String DISABLE_PROPAGATION_UT_TAG = "DISABLE_PROPAGATION";
}
