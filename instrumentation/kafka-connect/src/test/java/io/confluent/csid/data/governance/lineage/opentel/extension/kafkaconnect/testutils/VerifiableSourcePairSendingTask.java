/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.tools.VerifiableSourceTask;
import org.apache.kafka.tools.ThroughputThrottler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Based on {@link VerifiableSourceTask}, adapted to send only 2 events and return empty list on
 * subsequent polls to reduce unnecessary trace / span noise in tests.
 *
 * @see VerifiableSourceTask
 * @see VerifiableSourceConnector
 */
public class VerifiableSourcePairSendingTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(VerifiableSourcePairSendingTask.class);

  public static final String NAME_CONFIG = "name";
  public static final String ID_CONFIG = "id";
  public static final String TOPIC_CONFIG = "topic";
  public static final String THROUGHPUT_CONFIG = "throughput";

  protected static final String ID_FIELD = "id";
  protected static final String SEQNO_FIELD = "seqno";

  protected static final int NUMBER_OF_EVENTS_TO_GENERATE = 2;

  private static final ObjectMapper JSON_SERDE = new ObjectMapper();

  protected String name; // Connector name
  protected int id; // Task ID
  protected String topic;
  protected Map<String, Integer> partition;
  protected long startingSeqno;
  protected long seqno;
  protected ThroughputThrottler throttler;

  boolean sent = false;

  @Override
  public String version() {
    return new VerifiableSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    final long throughput;
    try {
      name = props.get(NAME_CONFIG);
      id = Integer.parseInt(props.get(ID_CONFIG));
      topic = props.get(TOPIC_CONFIG);
      throughput = Long.parseLong(props.get(THROUGHPUT_CONFIG));
    } catch (NumberFormatException e) {
      throw new ConnectException("Invalid VerifiableSourceTask configuration", e);
    }

    partition = Collections.singletonMap(ID_FIELD, id);
    Map<String, Object> previousOffset = this.context.offsetStorageReader().offset(partition);
    if (previousOffset != null) {
      seqno = (Long) previousOffset.get(SEQNO_FIELD) + 1;
    } else {
      seqno = 0;
    }
    startingSeqno = seqno;
    throttler = new ThroughputThrottler(throughput, System.currentTimeMillis());

    log.info("Started VerifiableSourceTask {}-{} producing to topic {} resuming from seqno {}",
        name, id, topic, startingSeqno);
  }

  /**
   * main method of the Source Task - on first call - generates and returns 2 events, on subsequent
   * calls returns empty result list to reduce unnecessary trace / span noise in tests.
   */
  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    long sendStartMs = System.currentTimeMillis();
    if (throttler.shouldThrottle(seqno - startingSeqno, sendStartMs)) {
      throttler.throttle();
    }
    List<SourceRecord> result = new ArrayList<>();
    //Only ever send 2 messages - helps with test stability
    if (sent) {
      return result;
    }
    for (int i = 0; i < NUMBER_OF_EVENTS_TO_GENERATE; i++) {
      Map<String, Long> ccOffset = Collections.singletonMap(SEQNO_FIELD, seqno);
      SourceRecord srcRecord = new SourceRecord(partition, ccOffset, topic,
          Schema.INT32_SCHEMA, id, Schema.INT64_SCHEMA, seqno);
      result.add(srcRecord);
      seqno++;
    }
    sent = true;
    return result;
  }

  @Override
  public void commitRecord(SourceRecord record, RecordMetadata metadata)
      throws InterruptedException {
    Map<String, Object> data = new HashMap<>();
    data.put("name", name);
    data.put("task", id);
    data.put("topic", this.topic);
    data.put("time_ms", System.currentTimeMillis());
    data.put("seqno", record.value());
    data.put("committed", true);

    String dataJson;
    try {
      dataJson = JSON_SERDE.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      dataJson = "Bad data can't be written as json: " + e.getMessage();
    }
    System.out.println(dataJson);
  }

  @Override
  public void stop() {
    throttler.wakeup();
  }
}
