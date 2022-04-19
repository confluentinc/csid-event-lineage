/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.INT_SIZE_BYTES;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

class HeaderSerdeTest {

  @Test
  void put() {
    Charset charset = StandardCharsets.UTF_8;
    String key = "key";
    byte[] keyBytes = key.getBytes(charset);
    byte[] value = "value".getBytes(charset);
    int length = INT_SIZE_BYTES + key.getBytes(charset).length + INT_SIZE_BYTES + value.length;

    byte[] expectedSerialized = ByteBuffer.allocate(length).putInt(keyBytes.length).put(keyBytes)
        .putInt(value.length).put(value).array();
    Header header = new RecordHeader(key, value);
    HeaderSerde headerSerde = new HeaderSerde(header, charset);

    assertThat(headerSerde.length()).isEqualTo(length);
    ByteBuffer buffer = ByteBuffer.allocate(length);
    headerSerde.put(buffer);

    byte[] serialized = buffer.array();
    assertThat(serialized).isEqualTo(expectedSerialized);
  }

  @Test
  void toHeader() {
    Charset charset = StandardCharsets.UTF_8;
    String key = "key";
    byte[] keyBytes = key.getBytes(charset);
    byte[] value = "value".getBytes(charset);
    Header expectedHeader = new RecordHeader(key, value);

    int length = INT_SIZE_BYTES + key.getBytes(charset).length + INT_SIZE_BYTES + value.length;

    ByteBuffer headerBuffer = ByteBuffer.allocate(length).putInt(keyBytes.length).put(keyBytes)
        .putInt(value.length).put(value);
    headerBuffer.rewind();

    HeaderSerde headerSerde = new HeaderSerde(headerBuffer);

    assertThat(headerSerde.length()).isEqualTo(length);

    assertThat(headerSerde.toHeader(charset)).isEqualTo(expectedHeader);
  }
}