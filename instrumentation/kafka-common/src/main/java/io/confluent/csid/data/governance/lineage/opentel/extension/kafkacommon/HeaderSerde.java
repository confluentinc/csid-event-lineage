/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.INT_SIZE_BYTES;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Header serialization / deserialization wrapper
 * <p>
 * Serialized format -
 * <pre>
 *     key_length(byte[4]) + key(byte[key.length]) + value_length(byte[4]) + value(byte[value.length])
 * </pre>
 */

public class HeaderSerde {

  private final byte[] key;
  private final byte[] value;
  private final int length;

  /**
   * @return serialized header bytes length
   */
  public int length() {
    return length;
  }

  /**
   * Reads in bytes from byte buffer and constructs HeaderSerde wrapper for deserialization
   *
   * @param buffer holding serialized header
   */
  public HeaderSerde(final ByteBuffer buffer) {
    final int keySize = buffer.getInt();
    key = new byte[keySize];
    buffer.get(key);

    final int valueSize = buffer.getInt();
    value = new byte[valueSize];
    buffer.get(value);

    length = INT_SIZE_BYTES + key.length + INT_SIZE_BYTES + value.length;
  }

  /**
   * Constructs HeaderSerde wrapper holding byte serialized header data using specified charset for
   * key String to byte serialization
   *
   * @param header  Header
   * @param charset used for key serialization to bytes
   */
  public HeaderSerde(final Header header, Charset charset) {
    key = header.key().getBytes(charset);
    value = header.value();

    length = INT_SIZE_BYTES + key.length + INT_SIZE_BYTES + value.length;
  }

  /**
   * Put serialized header byte[] into ByteBuffer.
   * <p>
   * Format -
   * key_length(byte[4])+key(byte[key.length])+value_length(byte[4])+value(byte[value.length])
   *
   * @param buffer to put the serialized header to.
   */
  public void put(final ByteBuffer buffer) {
    buffer.putInt(key.length)
        .put(key)
        .putInt(value.length)
        .put(value);
  }

  /**
   * Return deserialized header
   *
   * @param charset used for key deserialization to String
   * @return deserialized header as {@link RecordHeader}
   */
  public Header toHeader(Charset charset) {
    return new RecordHeader(
        new String(key, charset),
        value
    );
  }

}
