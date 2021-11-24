/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonTestValuePojoSerde implements Serializer<TestValuePojo>,
    Deserializer<TestValuePojo>, Serde<TestValuePojo> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
  }

  @SuppressWarnings("unchecked")
  @Override
  public TestValuePojo deserialize(final String topic, final byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      return OBJECT_MAPPER.readValue(data, TestValuePojo.class);
    } catch (final IOException e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public byte[] serialize(final String topic, final TestValuePojo data) {
    if (data == null) {
      return null;
    }

    try {
      return OBJECT_MAPPER.writeValueAsBytes(data);
    } catch (final Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public Serializer<TestValuePojo> serializer() {
    return this;
  }

  @Override
  public Deserializer<TestValuePojo> deserializer() {
    return this;
  }
}


