/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import lombok.experimental.UtilityClass;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Singleton instances used by Kafka Streams instrumentation extension
 */
@UtilityClass
public class Singletons {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  //additional serializer initialization
  static {
    SimpleModule module = new SimpleModule();
    module.addSerializer(ValueAndTimestamp.class, new JsonSerializer<ValueAndTimestamp>() {
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

  public static ObjectMapper objectMapper() {
    return OBJECT_MAPPER;
  }
}
