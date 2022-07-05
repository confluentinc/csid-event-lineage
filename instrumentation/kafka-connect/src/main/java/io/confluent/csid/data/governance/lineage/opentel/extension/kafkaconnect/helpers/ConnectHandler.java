/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeadersHandler;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Headers;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
public class ConnectHandler {

  HeadersHandler headersHandler;

  /**
   * Capture configured whitelisted headers to current span. Assumption is that header values are
   * Strings stored as byte[].
   *
   * @param headersToCapture Headers to capture
   * @param headerEncoding   Encoding to use for String to byte conversion
   */
  public void captureConnectHeadersToCurrentSpan(Headers headersToCapture, Charset headerEncoding) {
    if (headersToCapture == null) {
      return;
    }
    List<Header> convertedHeaders = new ArrayList<>();
    StreamSupport.stream(headersToCapture.spliterator(), false)
        .forEach(connectHeader -> addToHeadersList(connectHeader.key(),
            convertHeaderValue(connectHeader, headerEncoding), convertedHeaders));
    headersHandler
        .captureWhitelistedHeadersAsAttributesToCurrentSpan(
            convertedHeaders.toArray(Header[]::new));
  }

  /**
   * Convert header value from ConnectHeader type to just plain byte[] value. Attempting to check
   * for schema and indication if it's a byte[] value or String value.
   *
   * @param header         Connect Header to get value from
   * @param headerEncoding encoding to use for String to byte[] conversion.
   * @return byte[] header value
   */
  public byte[] convertHeaderValue(org.apache.kafka.connect.header.Header header,
      Charset headerEncoding) {
    if (header == null || header.value() == null) {
      return null;
    }
    if (header.value() instanceof SchemaAndValue) {
      SchemaAndValue schemaAndValue = (SchemaAndValue) header.value();
      if (schemaAndValue.schema().type() == Type.BYTES) {
        return (byte[]) schemaAndValue.value();
      } else {
        return schemaAndValue.value().toString()
            .getBytes(headerEncoding);
      }
    }
    if (header.schema() != null) {
      if (header.schema().type() == Type.BYTES || header.value() instanceof byte[]) {
        return (byte[]) header.value();
      }
    }
    return header.value().toString()
        .getBytes(headerEncoding);
  }

  private void addToHeadersList(String key, byte[] val, List<Header> headersList) {
    if (key != null && val != null && key.length() > 0 && val.length > 0) {
      headersList.add(new RecordHeader(key, val));
    }
  }
}
