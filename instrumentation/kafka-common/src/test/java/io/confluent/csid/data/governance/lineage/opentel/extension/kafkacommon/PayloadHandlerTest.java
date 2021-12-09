/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHandler.FLATTENED_PAYLOAD_ATTRIBUTE_FORMAT;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHandler.KEY_ATTRIBUTE_PREFIX;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHandler.VALUE_ATTRIBUTE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class PayloadHandlerTest {

  ObjectMapperUtil mockObjectMapperUtil = mock(ObjectMapperUtil.class);
  PayloadHandler classUnderTest = new PayloadHandler(mockObjectMapperUtil);
  String nullString = "null";

  @Test
  void captureKeyValuePayloadsToSpan() {
    Span mockSpan = mock(Span.class);

    String key = "keyTest";
    String value = "valueTest";
    setupFlattenPayloadMocks(key, value);

    classUnderTest.captureKeyValuePayloadsToSpan(key, value, mockSpan);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.raw.key"), key);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.key"), key);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.raw.value"), value);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.value"), value);
    verifyNoMoreInteractions(mockSpan);
  }

  @Test
  void captureKeyValuePayloadsToSpanCapturesNullKeyAsNullString() {
    Span mockSpan = mock(Span.class);

    String key = null;
    String value = "valueTest";
    setupFlattenPayloadMocks(key, value);

    classUnderTest.captureKeyValuePayloadsToSpan(key, value, mockSpan);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.raw.key"), nullString);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.key"), nullString);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.raw.value"), value);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.value"), value);
    verifyNoMoreInteractions(mockSpan);
  }

  @Test
  void captureKeyValuePayloadsToSpanCapturesNullValueAsNullString() {
    Span mockSpan = mock(Span.class);

    String key = "keyTest";
    String value = null;
    setupFlattenPayloadMocks(key, value);

    classUnderTest.captureKeyValuePayloadsToSpan(key, value, mockSpan);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.raw.key"), key);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.key"), key);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.raw.value"), nullString);
    verify(mockSpan).setAttribute(AttributeKey.stringKey("payload.value"), nullString);
    verifyNoMoreInteractions(mockSpan);
  }

  @Test
  void parseAndStorePayloadIntoPayloadHolder() {
    String key = "keyTest";
    String value = "valueTest";

    setupMapObjectToJsonMocks(key, value);

    classUnderTest.parseAndStorePayloadIntoPayloadHolder(key, value, PayloadHolder.PAYLOAD_HOLDER);
    PayloadHolder storedPayload = PayloadHolder.PAYLOAD_HOLDER.get();
    assertThat(storedPayload.getKey()).isEqualTo(key);
    assertThat(storedPayload.getValue()).isEqualTo(value);
  }

  @Test
  void parseAndStorePayloadIntoPayloadHolderParsesNullKeyAsNullString() {
    String key = null;
    String value = "valueTest";

    setupMapObjectToJsonMocks(key, value);

    classUnderTest.parseAndStorePayloadIntoPayloadHolder(key, value, PayloadHolder.PAYLOAD_HOLDER);
    PayloadHolder storedPayload = PayloadHolder.PAYLOAD_HOLDER.get();
    assertThat(storedPayload.getKey()).isEqualTo(nullString);
    assertThat(storedPayload.getValue()).isEqualTo(value);
  }

  @Test
  void parseAndStorePayloadIntoPayloadHolderParsesNullValueAsNullString() {
    String key = "keyTest";
    String value = null;

    setupMapObjectToJsonMocks(key, value);

    classUnderTest.parseAndStorePayloadIntoPayloadHolder(key, value, PayloadHolder.PAYLOAD_HOLDER);
    PayloadHolder storedPayload = PayloadHolder.PAYLOAD_HOLDER.get();
    assertThat(storedPayload.getKey()).isEqualTo(key);
    assertThat(storedPayload.getValue()).isEqualTo(nullString);
  }


  private void setupFlattenPayloadMocks(String key, String value) {
    String flattenedKeyKeyPrefix = String.format(FLATTENED_PAYLOAD_ATTRIBUTE_FORMAT,
        KEY_ATTRIBUTE_PREFIX);
    String flattenedValueKeyPrefix = String.format(FLATTENED_PAYLOAD_ATTRIBUTE_FORMAT,
        VALUE_ATTRIBUTE_PREFIX);

    String flattenedKeyKey = flattenedKeyKeyPrefix.substring(0, flattenedKeyKeyPrefix.length() - 1);
    String flattenedValueKey = flattenedValueKeyPrefix.substring(0,
        flattenedValueKeyPrefix.length() - 1);

    flattenPayloadMock(key, flattenedKeyKeyPrefix, flattenedKeyKey);
    flattenPayloadMock(value, flattenedValueKeyPrefix, flattenedValueKey);
  }

  private void flattenPayloadMock(String value, String prefix, String attributeKey) {
    String valueOrNullString = value != null ? value : nullString;

    when(mockObjectMapperUtil.flattenPayload(valueOrNullString, prefix)).thenReturn(
        Collections.singletonList(new SimpleEntry<>(attributeKey, valueOrNullString)));
  }

  private void setupMapObjectToJsonMocks(String key, String value) {
    mapObjectToJsonMock(key);
    mapObjectToJsonMock(value);
  }

  private void mapObjectToJsonMock(String value) {
    String valueOrNullString = value != null ? value : nullString;

    when(mockObjectMapperUtil.mapObjectToJSon(value)).thenReturn(valueOrNullString);

  }
}