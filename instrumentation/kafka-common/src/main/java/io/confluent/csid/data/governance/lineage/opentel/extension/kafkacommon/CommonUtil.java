/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CommonUtil {

  private static final String KEY_ATTRIBUTE_PREFIX = "key";
  private static final String VALUE_ATTRIBUTE_PREFIX = "value";
  private static final String RAW_PAYLOAD_ATTRIBUTE_FORMAT = "payload.raw.%s";
  private static final String FLATTENED_PAYLOAD_ATTRIBUTE_FORMAT = "payload.%s.";

  /**
   * Captures key and value payloads as Json objects and adds as Json object and as flattened
   * key/values to Span.
   * <p>
   * If payload object is a String - Json mapping step is skipped.
   *
   * @param key          key payload to capture
   * @param value        value payload to capture
   * @param objectMapper {@link ObjectMapper} to use for Json mapping step
   * @param spanToAddTo  {@link Span} to add attributes to
   */
  public static void captureKeyValuePayloadsToSpan(Object key,
      Object value, ObjectMapper objectMapper, Span spanToAddTo) {
    List<SimpleEntry<String, String>> payloadAttributes = new ArrayList<>(
        mapObjectPayloadToAttributeList(key, objectMapper, KEY_ATTRIBUTE_PREFIX));
    payloadAttributes.addAll(
        mapObjectPayloadToAttributeList(value, objectMapper, VALUE_ATTRIBUTE_PREFIX));
    addAttributesToSpan(payloadAttributes, spanToAddTo);
  }

  private static void addAttributesToSpan(List<SimpleEntry<String, String>> attributesToAdd,
      Span span) {
    for (SimpleEntry<String, String> attribute : attributesToAdd) {
      span.setAttribute(AttributeKey.stringKey(attribute.getKey()), attribute.getValue());
    }
  }

  private static List<SimpleEntry<String, String>> mapObjectPayloadToAttributeList(
      Object payloadObject, ObjectMapper objectMapper, String prefix) {
    String payloadString;
    if (payloadObject instanceof String) {
      payloadString = (String) payloadObject;
    } else {
      payloadString = ObjectMapperUtil.mapObjectToJSon(payloadObject, objectMapper);
    }

    List<SimpleEntry<String, String>> attributesList = new ArrayList<>();
    //Add payload as Json object
    attributesList.add(
        new SimpleEntry<>(String.format(RAW_PAYLOAD_ATTRIBUTE_FORMAT, prefix), payloadString));

    //Add payload as flattened key/value pairs
    attributesList.addAll(
        ObjectMapperUtil.flattenPayload(payloadString,
            String.format(FLATTENED_PAYLOAD_ATTRIBUTE_FORMAT, prefix)));

    return attributesList;
  }

  public static void parseAndStorePayloadIntoPayloadHolder(Object key, Object value,
      ObjectMapper objectMapper, ThreadLocal<PayloadHolder> payloadHolder) {
    payloadHolder.set(
        new PayloadHolder(
            ObjectMapperUtil.mapObjectToJSon(key, objectMapper),
            ObjectMapperUtil.mapObjectToJSon(value, objectMapper)
        )
    );
  }
}
