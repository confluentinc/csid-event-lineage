/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import java.time.Duration;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TestConstants {

  /**
   * Tests tagged with this tag are not propagating Tracing Context through Kafka message headers.
   * That way consumption of non-traced messages can be simulated - for scenarios where for example
   * Connect Replicator has tracing installed, but producing application does not.
   */
  public static final String DISABLE_PROPAGATION_UT_TAG = "DISABLE_PROPAGATION";

  public static class TIMEOUTS {

    public static final Integer LATCH_TIMEOUT_SECONDS = 120;
    public static final Integer DEFAULT_TIMEOUT_SECONDS = 15;
    public static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(
        DEFAULT_TIMEOUT_SECONDS);
    public static final Duration CONSUME_AWAIT_TIMEOUT = Duration.ofSeconds(30);
    public static final Duration CONNECT_STOP_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration POLL_INTERVAL = Duration.ofMillis(100);
  }
}
