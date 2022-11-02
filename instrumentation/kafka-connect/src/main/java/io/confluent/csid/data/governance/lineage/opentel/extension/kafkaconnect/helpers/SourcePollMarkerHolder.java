/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import lombok.Value;

/**
 * SourceTask poll method execution marker - SourceRecord constructor advice should only be executed
 * within SourceTask.poll method execution to capture current tracing context if set and correlate
 * it with SourceRecord. That way source-task can set that context as parent to preserve the trace
 * chain.
 */
@Value
public class SourcePollMarkerHolder {

  private static final ThreadLocal<Boolean> SOURCE_POLL_MARKER_HOLDER = ThreadLocal.withInitial(
      () -> false);

  public static void enableTracing() {
    SOURCE_POLL_MARKER_HOLDER.set(true);
  }

  public static boolean get() {
    return SOURCE_POLL_MARKER_HOLDER.get();
  }

  public static void disableTracing() {
    SOURCE_POLL_MARKER_HOLDER.set(false);
  }
}
