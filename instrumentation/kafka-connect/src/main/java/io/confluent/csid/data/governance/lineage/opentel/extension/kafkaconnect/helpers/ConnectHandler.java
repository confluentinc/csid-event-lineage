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
    List<Header> convertedHeaders = new ArrayList<>();
    StreamSupport.stream(headersToCapture.spliterator(), false)
        .forEach(connectHeader -> addToHeadersList(connectHeader.key(),
            convertHeaderValue(connectHeader.value(), headerEncoding), convertedHeaders));
    headersHandler
        .captureWhitelistedHeadersAsAttributesToCurrentSpan(
            convertedHeaders.toArray(Header[]::new));
  }


  private static byte[] convertHeaderValue(Object value, Charset headerEncoding) {
    if (value == null) {
      return null;
    }
    if (value instanceof byte[]) {
      return (byte[]) value;
    } else {
      return value.toString()
          .getBytes(headerEncoding);
    }
  }

  private static void addToHeadersList(String key, byte[] val, List<Header> headersList) {
    if (key != null && val != null && key.length() > 0 && val.length > 0) {
      headersList.add(new RecordHeader(key, val));
    }
  }
}
