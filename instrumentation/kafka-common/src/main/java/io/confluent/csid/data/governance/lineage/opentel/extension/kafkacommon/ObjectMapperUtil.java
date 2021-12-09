/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.JsonFlattener;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Bytes;

/**
 * Payload mapping to JSON utility methods
 */
public class ObjectMapperUtil {

  private final ObjectMapper objectMapper;

  public ObjectMapperUtil(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /**
   * Maps given object to Json - using supplied {@link ObjectMapper} with any additional registered
   * serializers.
   * <p>
   * Strips additional double quotes that Jackson {@link ObjectMapper} adds by default if object is
   * a String or {@link UUID} - returns unchanged.
   * <p>
   * If object is of {@link Bytes} type - unwraps inner byte array before passing to {@link
   * ObjectMapper}.
   * <p>
   * On {@link JsonProcessingException} returns error string with exception details instead of
   * mapped object.
   *
   * @param object       - object to map to Json
   * @return Json representation of object as String
   */
  public String mapObjectToJSon(Object object) {
    try {
      if (object instanceof String) {
        return (String) object;
      }
      if (object instanceof UUID) {
        return object.toString();
      }
      if (object instanceof Bytes) {
        object = ((Bytes) object).get();
      }
      String result = objectMapper.writeValueAsString(object);
      if (startsAndEndsWithDoubleQuotes(result)) {
        return result.substring(1, result.length() - 1);
      } else {
        return result;
      }

    } catch (JsonProcessingException e) {
      if (object != null) {
        return "ERROR PARSING PAYLOAD for " + object + " input object, Exception: " + e;
      } else { //Should not ever happen as null input object is parsed as "null" string. Adding for potentially unforeseen edge case.
        return "ERROR PARSING PAYLOAD for NULL input object, Exception: " + e;
      }
    }
  }

  /**
   * Checks if given String starts and ends with double quotes
   *
   * @param stringToCheck String to check
   * @return boolean indicating if String starts and ends with double quotes
   */
  private boolean startsAndEndsWithDoubleQuotes(String stringToCheck) {
    return stringToCheck.startsWith("\"") && stringToCheck.endsWith("\"");
  }

  /**
   * Parses Json String into list of key / value pairs adding attribute prefix.
   * <p>
   * For example - given Json of:
   * <pre>
   * {
   *    "compositeKey" : {
   *        "innerKey":"keyValue",
   *        "innerKey2":"key2Value"
   *     }
   * }
   * and attribute prefix of "payload.key." - results in list with two key / value pairs:
   * [
   *    "payload.key.compositeKey.innerKey" = "keyValue",
   *    "payload.key.compositeKey.innerKey2" ="key2Value"
   * ]
   * </pre>
   * If string is not valid Json - returns singleton list with key = attribute prefix stripping
   * trailing dot and value = given string
   *
   * @param json            to parse and flatten
   * @param attributePrefix prefix for resulting keys
   * @return list of key / value pairs
   */
   List<SimpleEntry<String, String>> flattenPayload(String json,
      String attributePrefix) {
    if (null == json) {
      return Collections.singletonList(
          new SimpleEntry<>(getSingleValuePrefix(attributePrefix), "null"));
    }

    if (!json.trim().startsWith("{")) { //Simple check if string is likely to be a json string
      return Collections.singletonList(
          new SimpleEntry<>(getSingleValuePrefix(attributePrefix), json));
    }

    Map<String, Object> payloadMap;
    try {
      payloadMap = JsonFlattener.flattenAsMap(json);
    } catch (RuntimeException suppressed) { // Exception thrown if string does start with { but is not a valid Json.
      //Return input string without flattening in such case.
      return Collections.singletonList(
          new SimpleEntry<>(getSingleValuePrefix(attributePrefix), json));
    }

    return payloadMap.entrySet().stream()
        .filter(entry -> entry.getValue() != null)
        .map(entry -> new SimpleEntry<>(attributePrefix + entry.getKey(),
            entry.getValue().toString()))
        .collect(Collectors.toList());

  }

  /**
   * Strips trailing dot from attribute prefix if present
   *
   * @param attributePrefix
   * @return attributePrefix with trailing dot stripped
   */
  private String getSingleValuePrefix(String attributePrefix) {
    if (attributePrefix.endsWith(".")) {
      return attributePrefix.substring(0, attributePrefix.length() - 1);
    } else {
      return attributePrefix;
    }
  }
}
