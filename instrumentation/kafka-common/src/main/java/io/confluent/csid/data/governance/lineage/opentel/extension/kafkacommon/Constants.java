/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

import io.opentelemetry.api.common.AttributeKey;
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

  public static final AttributeKey<String> SERVICE_NAME_KEY = stringKey("service.name");
  public static final AttributeKey<String> CLUSTER_ID_KEY = stringKey("cluster.id");

  /**
   * Constants for building span names
   */
  public class SpanNames {

    public static final String SOURCE_TASK = "source-task";

    public static final String SINK_TASK = "sink-task";

    /**
     * Span name format - "topic source-task"  "topic sink-task" - i.e. "OutputTopic-123
     * source-task"
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
     * State store Cache Put operation name - used by all put operations - when performed on Cache
     * layer of the state store
     */
    public static final String STATE_STORE_CACHE_PUT = "state-store-cache-put";

    /**
     * State store delete operation name - used by delete operations
     */
    public static final String STATE_STORE_DELETE = "state-store-delete";

    /**
     * State store Cache delete operation name - used by delete operations - when performed on Cache
     * layer of the State Store
     */
    public static final String STATE_STORE_CACHE_DELETE = "state-store-delete";

    /**
     * State store remove operation name - used by session store remove operation
     */
    public static final String STATE_STORE_REMOVE = "state-store-remove";

    /**
     * State store Cache Remove operation name - used by session store remove operation - when
     * performed on Cache layer of the state store
     */
    public static final String STATE_STORE_CACHE_REMOVE = "state-store-cache-remove";

    /**
     * State store cache flush operation name - used by cache flush operation
     */
    public static final String STATE_STORE_FLUSH = "state-store-cache-flush";

    public static String PRODUCE_CONSUME_TASK_FORMAT = "%s %s";
    public static final String PRODUCER_SEND = "send";

    public static final String CONSUMER_PROCESS = "process";
  }
}
