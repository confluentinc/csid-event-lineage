/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;

public class PayloadHandler {

  static final String KEY_ATTRIBUTE_PREFIX = "key";
  static final String VALUE_ATTRIBUTE_PREFIX = "value";
  static final String RAW_PAYLOAD_ATTRIBUTE_FORMAT = "payload.raw.%s";
  static final String FLATTENED_PAYLOAD_ATTRIBUTE_FORMAT = "payload.%s.";

  private final ObjectMapperUtil objectMapperUtil;

  public PayloadHandler(ObjectMapperUtil objectMapperUtil) {
    this.objectMapperUtil = objectMapperUtil;
  }

  /**
   * Captures key and value payloads as Json objects and adds as Json object and as flattened
   * key/values to Span.
   * <p>
   * If payload object is a String - Json mapping step is skipped.
   *
   * @param key         key payload to capture
   * @param value       value payload to capture
   * @param spanToAddTo {@link Span} to add attributes to
   */
  public void captureKeyValuePayloadsToSpan(Object key,
      Object value, Span spanToAddTo) {
    List<SimpleEntry<String, String>> payloadAttributes = new ArrayList<>(
        mapObjectPayloadToAttributeList(key, KEY_ATTRIBUTE_PREFIX));
    payloadAttributes.addAll(
        mapObjectPayloadToAttributeList(value, VALUE_ATTRIBUTE_PREFIX));
    addAttributesToSpan(payloadAttributes, spanToAddTo);
  }

  /**
   * Serialize key and value payloads to JSon and store in ThreadLocal {@link PayloadHolder}
   * @param key payload to serialize and store
   * @param value payload to serialize and store
   * @param payloadHolder ThreadLocal to store the payloads in.
   */
  public void parseAndStorePayloadIntoPayloadHolder(Object key, Object value,
      ThreadLocal<PayloadHolder> payloadHolder) {
    payloadHolder.set(
        new PayloadHolder(
            objectMapperUtil.mapObjectToJSon(key),
            objectMapperUtil.mapObjectToJSon(value)
        )
    );
  }

  private void addAttributesToSpan(List<SimpleEntry<String, String>> attributesToAdd,
      Span span) {
    for (SimpleEntry<String, String> attribute : attributesToAdd) {
      span.setAttribute(AttributeKey.stringKey(attribute.getKey()), attribute.getValue());
    }
  }

  private List<SimpleEntry<String, String>> mapObjectPayloadToAttributeList(
      Object payloadObject, String prefix) {
    String payloadString;
    if (null == payloadObject) {
      payloadString = "null";
    } else if (payloadObject instanceof String) {
      payloadString = (String) payloadObject;
    } else {
      payloadString = objectMapperUtil.mapObjectToJSon(payloadObject);
    }

    List<SimpleEntry<String, String>> attributesList = new ArrayList<>();
    //Add payload as Json object
    attributesList.add(
        new SimpleEntry<>(String.format(RAW_PAYLOAD_ATTRIBUTE_FORMAT, prefix), payloadString));

    //Add payload as flattened key/value pairs
    attributesList.addAll(
        objectMapperUtil.flattenPayload(payloadString,
            String.format(FLATTENED_PAYLOAD_ATTRIBUTE_FORMAT, prefix)));

    return attributesList;
  }
}
