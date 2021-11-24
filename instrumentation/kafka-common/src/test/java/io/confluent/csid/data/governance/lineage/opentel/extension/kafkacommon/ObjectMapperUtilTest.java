/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import lombok.Value;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ObjectMapperUtilTest {

  ObjectMapper objectMapper = new ObjectMapper();

  @Test
  @DisplayName("Test mapObjectToJSon for Null object returns \"null\" String")
  void testMapObjectToJSonForNullObjectReturnsNullString() {
    assertThat(ObjectMapperUtil.mapObjectToJSon(null, objectMapper))
        .isEqualTo("null");
  }

  @Test
  @DisplayName("Test mapObjectToJSon for ByteArray object returns Base64 encoded String")
  void testMapObjectToJSonForByteArrayReturnsBase64EncodedValue() {
    byte[] byteArrayObject = new byte[]{41, 32, 12};
    String expectedResult = Base64.getEncoder().encodeToString(byteArrayObject);
    assertThat(ObjectMapperUtil.mapObjectToJSon(byteArrayObject, objectMapper))
        .isEqualTo(expectedResult);
  }

  @Test
  @DisplayName("Test mapObjectToJSon for Bytes object returns Base64 encoded String")
  void testMapObjectToJSonForBytesObjectMapsWrappedByteArray() {
    byte[] byteArrayObject = new byte[]{41, 32, 12};
    String expectedResult = Base64.getEncoder().encodeToString(byteArrayObject);
    Bytes wrappedByteArray = new Bytes(byteArrayObject);
    assertThat(ObjectMapperUtil.mapObjectToJSon(wrappedByteArray, objectMapper))
        .isEqualTo(expectedResult);
  }

  @Test
  @DisplayName("Test mapObjectToJSon for String returns it unchanged")
  void testMapObjectToJSonForStringReturnsItUnchanged() {
    String inputString = "Test string with special chars \"\n\\;{}[]";
    assertThat(ObjectMapperUtil.mapObjectToJSon(inputString, objectMapper))
        .isEqualTo(inputString);
  }

  @Test
  @DisplayName("Test mapObjectToJSon for UUID returns it as String")
  void testMapObjectToJSonForUUIDReturnsUUIDAsString() {
    UUID input = UUID.randomUUID();
    assertThat(ObjectMapperUtil.mapObjectToJSon(input, objectMapper))
        .isEqualTo(input.toString());
  }

  @Test
  @DisplayName("Test mapObjectToJSon for Pojo returns Json String without extra quotes")
  void testMapObjectToJSonForPojoReturnsJsonStringWithoutAdditionalQuotes() {
    PojoTest input = new PojoTest("stringValue", 10, Arrays.asList("listItem1", "listItem2"));
    assertThat(ObjectMapperUtil.mapObjectToJSon(input, objectMapper))
        .isEqualTo(
            "{\"stringField\":\"stringValue\",\"intField\":10,\"stringList\":[\"listItem1\",\"listItem2\"]}");
  }

  @Test
  @DisplayName("Test mapObjectToJSon for Pojo returns Error Parsing Payload message on exception")
  void testMapObjectToJSonReturnsErrorParsingMessageWhenObjectCannotBeParsed() {
    Object objectWithoutMapper = new Object();
    assertDoesNotThrow(() -> ObjectMapperUtil.mapObjectToJSon(objectWithoutMapper, objectMapper));
    assertThat(ObjectMapperUtil.mapObjectToJSon(objectWithoutMapper, objectMapper))
        .startsWith("ERROR PARSING PAYLOAD for java.lang.Object");
  }

  @Test
  @DisplayName("Test flattenPayload for Json String returns List of key/value pairs")
  void testFlattenJsonForJsonStringReturnsListOfPairs() {
    String json = "{\"stringField\":\"stringValue\",\"intField\":10,\"stringList\":[\"listItem1\",\"listItem2\"]}";
    assertThat(ObjectMapperUtil.flattenPayload(json, "test.")).containsExactly(
        new SimpleEntry<>("test.stringField", "stringValue"),
        new SimpleEntry<>("test.intField", "10"),
        new SimpleEntry<>("test.stringList[0]", "listItem1"),
        new SimpleEntry<>("test.stringList[1]", "listItem2")
    );
  }

  @Test
  @DisplayName("Test flattenPayload for non-Json String strips leading dot from attribute prefix")
  void testFlattenJsonForNonJsonStringStripsLeadingDotFromAttributePrefix() {
    String json = "testValue";
    assertThat(ObjectMapperUtil.flattenPayload(json, "test.")).containsExactly(
        new SimpleEntry<>("test", "testValue")
    );
  }

  @Test
  @DisplayName("Test flattenPayload for non-Json String uses prefix as-is if not ending with dot")
  void testFlattenJsonForNonJsonStringUsesPrefixAsIsIfItsNotEndingWithDot() {
    String json = "testValue";
    assertThat(ObjectMapperUtil.flattenPayload(json, "test-key")).containsExactly(
        new SimpleEntry<>("test-key", "testValue")
    );
  }

  @Test
  @DisplayName("Test flattenPayload for non-Json String starting with '{' strips leading dot from attribute prefix")
  void testFlattenJsonForNonJsonStringStartingWithOpenBracketStripsLeadingDotFromAttributePrefix() {
    String json = "{testValue}";
    assertThat(ObjectMapperUtil.flattenPayload(json, "test.")).containsExactly(
        new SimpleEntry<>("test", "{testValue}")
    );
  }

  @Test
  @DisplayName("Test flattenPayload for Null input returns attribute with \"null\" String value")
  void testFlattenJsonForNullValueReturnsAttributeWithNullStringValue() {
    String json = null;
    assertThat(ObjectMapperUtil.flattenPayload(json, "test.")).containsExactly(
        new SimpleEntry<>("test", "null")
    );
  }

  @Value
  static class PojoTest {

    String stringField;
    int intField;
    List<String> stringList;
  }
}