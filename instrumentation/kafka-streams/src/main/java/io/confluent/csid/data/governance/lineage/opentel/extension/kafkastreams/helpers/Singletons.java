/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ObjectMapperUtil;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.OpenTelemetryWrapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHandler;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanHandler;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers;
import java.io.IOException;
import lombok.experimental.UtilityClass;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.MeteredWindowStore;

/**
 * Singleton instances used by Kafka Streams instrumentation extension
 */
@UtilityClass
public class Singletons {

  /**
   * ThreadLocal holding Trace ID read from value retrieved from state store
   */
  public static final ThreadLocal<String> STORED_TRACE_ID_HOLDER = new ThreadLocal<>();

  /**
   * ThreadLocal holding Message Key for StateStore Fetch executed during KStream to KStream Join
   * see {@link org.apache.kafka.streams.kstream.internals.KStreamKStreamJoin.KStreamKStreamJoinProcessor#process}
   * and {@link MeteredWindowStore#fetch(Object, long, long)}
   */
  public static final ThreadLocal<Object> FETCH_KEY_HOLDER = new ThreadLocal<>();

  /**
   * Singleton {@link ObjectMapper} instance used to serialize key / value payloads into Json for
   * capture as trace attributes.
   */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final ObjectMapperUtil OBJECT_MAPPER_UTIL = new ObjectMapperUtil(objectMapper());

  private static final PayloadHandler PAYLOAD_HANDLER = new PayloadHandler(objectMapperUtil());

  private static final OpenTelemetryWrapper OPEN_TELEMETRY_WRAPPER = new OpenTelemetryWrapper();

  private static final SpanHandler SPAN_HANDLER = new SpanHandler(openTelemetryWrapper(),
      Constants.INSTRUMENTATION_NAME);
  private static final StateStorePropagationHelpers STATE_STORE_PROPAGATION_HELPERS = new StateStorePropagationHelpers(
      openTelemetryWrapper(), payloadHandler(), spanHandler());

  //additional serializer initialization
  static {
    SimpleModule module = new SimpleModule();
    module.addSerializer(ValueAndTimestamp.class, new JsonSerializer<>() {
      @Override
      public void serialize(ValueAndTimestamp valueAndTimestamp, JsonGenerator jGen,
          SerializerProvider serializerProvider) throws IOException {
        jGen.writeStartObject();
        jGen.writeObjectField("value", valueAndTimestamp.value());
        jGen.writeNumberField("timestamp", valueAndTimestamp.timestamp());
        jGen.writeEndObject();
      }
    });
    OBJECT_MAPPER.registerModule(module);
  }

  /**
   * Singleton instance of {@link ObjectMapper} with additional JsonSerializers configured see
   * initialization in {@link Singletons} class
   */
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

  public static StateStorePropagationHelpers stateStorePropagationHelpers() {
    return STATE_STORE_PROPAGATION_HELPERS;
  }

  public static SpanHandler spanHandler() {
    return SPAN_HANDLER;
  }
}
