/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeaderCaptureConfiguration.HEADER_CAPTURE_WHITELIST_PROP;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeaderCaptureConfiguration.HEADER_CHARSET_PROP;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeaderCaptureConfiguration.HEADER_PROPAGATION_WHITELIST_PROP;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ConfigurationReloader;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeaderCaptureConfiguration;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

public class HeaderPropagationTestUtils {

  public static final Charset CHARSET_UTF_8 = StandardCharsets.UTF_8;

  public static final Header CAPTURED_PROPAGATED_HEADER = new RecordHeader(
      "captured_propagated_header",
      "captured_propagated_headerval".getBytes(CHARSET_UTF_8));
  public static final Header CAPTURED_PROPAGATED_HEADER_2 = new RecordHeader(
      "captured_propagated_header2",
      "captured_propagated_headerval2".getBytes(CHARSET_UTF_8));
  public static final Header NOT_CAPTURED_PROPAGATED_HEADER = new RecordHeader(
      "non_captured_propagated_header",
      "non_captured_propagated_headerval".getBytes(CHARSET_UTF_8));

  public static final Header[] CAPTURE_WHITELISTED_HEADERS = new Header[]{
      new RecordHeader("captured_header1", "captured_headerval1".getBytes(CHARSET_UTF_8)),
      new RecordHeader("captured_header2", "captured_headerval2".getBytes(CHARSET_UTF_8)),
      CAPTURED_PROPAGATED_HEADER,
      CAPTURED_PROPAGATED_HEADER_2
  };

  public static final Header[] CAPTURED_PROPAGATED_HEADERS = new Header[]{
      CAPTURED_PROPAGATED_HEADER,
      CAPTURED_PROPAGATED_HEADER_2};

  public static final Header[] PROPAGATION_WHITELISTED_HEADERS = new Header[]{
      CAPTURED_PROPAGATED_HEADER,
      CAPTURED_PROPAGATED_HEADER_2,
      NOT_CAPTURED_PROPAGATED_HEADER
  };
  public static final Header[] NOT_WHITELISTED_HEADERS = new Header[]{
      new RecordHeader("non_captured_non_propagated_header1",
          "non_captured_non_propagated_headerval1".getBytes(CHARSET_UTF_8)),
      new RecordHeader("non_captured_non_propagated_header2",
          "non_captured_non_propagated_headerval2".getBytes(CHARSET_UTF_8))
  };

  public static void setupHeaderConfiguration(HeaderCaptureConfiguration headerCaptureConfiguration) {
    System.setProperty(HEADER_CAPTURE_WHITELIST_PROP,
        Arrays.stream(CAPTURE_WHITELISTED_HEADERS).map(Header::key)
            .collect(Collectors.joining(",")));
    System.setProperty(HEADER_PROPAGATION_WHITELIST_PROP,
        Arrays.stream(PROPAGATION_WHITELISTED_HEADERS).map(Header::key)
            .collect(Collectors.joining(",")));
    System.setProperty(HEADER_CHARSET_PROP, CHARSET_UTF_8.name());
    ConfigurationReloader.reloadHeaderCaptureConfiguration(headerCaptureConfiguration);
  }

  public static void cleanupHeaderConfiguration(HeaderCaptureConfiguration headerCaptureConfiguration) {
    System.clearProperty(HEADER_CAPTURE_WHITELIST_PROP);
    System.clearProperty(HEADER_PROPAGATION_WHITELIST_PROP);
    System.clearProperty(HEADER_CHARSET_PROP);
    ConfigurationReloader.reloadHeaderCaptureConfiguration(headerCaptureConfiguration);
  }
}
