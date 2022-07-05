/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {

  public static final String INSTRUMENTATION_NAME_KAFKA_STREAMS = "io.opentelemetry.kafka-streams-2.6";
  public static final String INSTRUMENTATION_NAME_KAFKA_CLIENTS = "io.opentelemetry.kafka-clients-0.11";

  public static final String HEADER_ATTRIBUTE_PREFIX = "headers.";
  /**
   * Opentelemetry trace header name
   */
  public static final String TRACING_HEADER = "traceparent";
  /**
   * Magic byte used to store/get trace id to/from byte[] serialized value
   */
  public static final byte[] TRACING_MAGIC_BYTES = new byte[]{'T', 'R', 'A', 'C', 'E'};
  /**
   * Size of int value in bytes
   */
  public static final int INT_SIZE_BYTES = Integer.BYTES;

  /**
   * Timestamp (long) value length in bytes
   */
  public static final int TIMESTAMP_LENGTH = Long.BYTES;

  /**
   * Constants for building span names
   */
  public class SpanNames {

    public static final String SOURCE_TASK = "source-task";

    public static final String SINK_TASK = "sink-task";

    /**
     * Span name format - "topic source-task"  "topic sink-task" - i.e. "OutputTopic-123 source-task"
     */
    public static final String TASK_SPAN_NAME_FORMAT = "%s %s";

    public static final String SMT = "SMT";
    /**
     * Span name format - "SMT smt-name" - i.e. "SMT HeaderExtractor"
     */
    public static final String SMT_SPAN_NAME_FORMAT = "%s %s";
    /**
     * Span name format - "operation state-store-name" - i.e. "state-store-get
     * value-aggregate-store"
     */
    public static final String STATE_STORE_SPAN_NAME_FORMAT = "%s %s";
    /**
     * State store Get operation name - used by all retrieval operations - get, fetch, fetchAll,
     * fetchRange
     */
    public static final String STATE_STORE_GET = "state-store-get";
    /**
     * State store Put operation name - used by all put operations - put, putIfAbsent, putAll
     */
    public static final String STATE_STORE_PUT = "state-store-put";
    /**
     * State store delete operation name - used by delete / remove operations - delete,
     * removeSession
     */
    public static final String STATE_STORE_DELETE = "state-store-delete";
  }
}
