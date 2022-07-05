/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TIMESTAMP_LENGTH;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.StateStorePropagationHelpers;
import java.nio.ByteBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.apache.kafka.streams.state.internals.RecordConverters;

/**
 * Handling of ValueAndTimestamp transformations for tracing - in multiple places KStreams library
 * appends timestamp or splits it to/from value on byte[] level. For tracing magic bytes have to be
 * preserved at start of byte[] so byte[] is rearranged to keep in sequence of TraceData Timestamp
 * ValueBytes.
 */
public class ValueAndTimestampHandler {

  private final StateStorePropagationHelpers stateStorePropagationHelpers;
  private final RecordConverter rearrangingRecordConverter = record -> {
    final byte[] rawValue = record.value();
    final long timestamp = record.timestamp();
    final byte[] rearrangedValue = rawValue == null ? null :
        rearrangeTimestampAndValue(record.value());
    final byte[] recordValue = (rawValue == null ? null :
        (rearrangedValue == null ? rawValue : rearrangedValue));
    return new ConsumerRecord<>(
        record.topic(),
        record.partition(),
        record.offset(),
        timestamp,
        record.timestampType(),
        record.checksum(),
        record.serializedKeySize(),
        record.serializedValueSize(),
        record.key(),
        recordValue,
        record.headers(),
        record.leaderEpoch()
    );
  };

  public ValueAndTimestampHandler(StateStorePropagationHelpers stateStorePropagationHelpers) {
    this.stateStorePropagationHelpers = stateStorePropagationHelpers;
  }

  /**
   * Gets raw bytes value from timestamp+value pair.
   * <p>
   * If Value is trace enabled - i.e. byte[] starts with Tracing magic bytes - assumes format of
   * TraceData Timestamp ValueBytes and returns TraceData ValueBytes with timestamp portion of
   * byte[] removed.
   * <p>
   * Returns null if byte[] does not start with Tracing magic bytes as that short circuits the
   * ValueAndTimestamp instrumentation advice to return to original behaviour.
   *
   * @param bytesValue byte[] holding timestamp, value and optionally trace data in sequence of
   *                   Trace Timestamp ValueBytes
   * @return Trace + Value bytes with timestamp removed or null if not trace enabled
   * <p>
   * See {@link org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer#rawValue(byte[])}
   */
  public byte[] rawValue(byte[] bytesValue) {
    if (null == bytesValue) {
      return null;
    }
    if (!stateStorePropagationHelpers.hasTracingInfoAttached(bytesValue)) {
      return null;
    }
    int originalDataLength =
        stateStorePropagationHelpers.getOriginalDataLength(bytesValue, 0) - TIMESTAMP_LENGTH;
    int traceDataLength = bytesValue.length - (originalDataLength + TIMESTAMP_LENGTH);

    ByteBuffer buffer = ByteBuffer.allocate(originalDataLength + traceDataLength)
        .put(bytesValue, 0, traceDataLength);
    buffer.put(bytesValue, traceDataLength + TIMESTAMP_LENGTH, originalDataLength);

    return buffer.array();
  }

  /**
   * Gets raw timestamp from timestamp+value pair
   * <p>
   * If bytesValue is trace enabled - i.e. byte[] starts with Tracing magic bytes - assumes format
   * of TraceData Timestamp ValueBytes and returns Timestamp portion of byte array.
   * <p>
   * If bytesValue is does not start with Tracing magic bytes - returns as is for original / default
   * timestamp extractor to process
   *
   * @param bytesValue byte[] holding timestamp, value and optionally trace data in sequence of
   *                   Trace Timestamp ValueBytes
   * @return Timestamp bytes (if traced value) or original bytes (if not traced).
   * <p>
   * See {@link org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer#rawTimestamp(byte[])}
   */
  public byte[] rawTimestamp(byte[] bytesValue) {
    if (null == bytesValue) {
      return null;
    }

    if (!stateStorePropagationHelpers.hasTracingInfoAttached(bytesValue)) {
      return bytesValue;
    }
    int originalDataLength =
        stateStorePropagationHelpers.getOriginalDataLength(bytesValue, 0) - TIMESTAMP_LENGTH;
    int traceDataLength = bytesValue.length - (originalDataLength + TIMESTAMP_LENGTH);
    return ByteBuffer.allocate(8).put(bytesValue, traceDataLength, TIMESTAMP_LENGTH)
        .array();
  }

  /**
   * Rearranges Timestamp + Trace + Value bytes into Trace + Timestamp + Value bytes to allow proper
   * working of tracing interceptors that check for Tracing magic bytes
   * <p>
   * If valueWithPrependedTimestamp byte[] has tracing magic bytes at position 8 - after the
   * prepended timestamp - then byte array is rearranged to format of [Trace][Timestamp][value]
   * <p>
   * If valueWithPrependedTimestamp byte[] doesn't have tracing magic bytes at position 8 - returns
   * unchanged.
   *
   * @param valueWithPrependedTimestamp byte[] of value optionally with Trace data prepended with
   *                                    timestamp bytes
   * @return rearranged timestamp + trace + value bytes in Trace+Timestamp+Value bytes order
   */
  public byte[] rearrangeTimestampAndValue(byte[] valueWithPrependedTimestamp) {
    if (valueWithPrependedTimestamp == null) {
      return null;
    }
    if (!stateStorePropagationHelpers.hasTracingInfoAttachedAtOffset(valueWithPrependedTimestamp,
        TIMESTAMP_LENGTH)) {
      return valueWithPrependedTimestamp;
    }
    int originalDataLength =
        stateStorePropagationHelpers.getOriginalDataLength(valueWithPrependedTimestamp,
            TIMESTAMP_LENGTH) - TIMESTAMP_LENGTH;
    int traceLength = valueWithPrependedTimestamp.length - (originalDataLength + TIMESTAMP_LENGTH);

    ByteBuffer bufferRearranged = ByteBuffer.allocate(valueWithPrependedTimestamp.length);
    bufferRearranged.put(valueWithPrependedTimestamp, TIMESTAMP_LENGTH,
        traceLength);  //put trace data first
    bufferRearranged.put(valueWithPrependedTimestamp, 0,
        TIMESTAMP_LENGTH);     //followed by timestamp
    bufferRearranged.put(valueWithPrependedTimestamp, traceLength + TIMESTAMP_LENGTH,
        originalDataLength); // original value data last.
    return bufferRearranged.array();
  }

  /**
   * RecordConverter performing Timestamp+Trace+Value rearrangement into Trace+Timestamp+Value
   * order. Used during state store restoration.
   *
   * @param wrappedConverter converter to wrap / combine with
   * @return converter chain - wrapped converter followed by rearranging converter.
   * <p>
   * See {@link RecordConverters#rawValueToTimestampedValue()} and {@link
   * org.apache.kafka.streams.processor.internals.StateManagerUtil#converterForStore(StateStore)}
   */
  public RecordConverter rearrangingRawValueToTimestampedValueConverter(
      RecordConverter wrappedConverter) {
    return record -> {
      ConsumerRecord<byte[], byte[]> converted = wrappedConverter.convert(record);
      return rearrangingRecordConverter.convert(converted);
    };
  }
}
