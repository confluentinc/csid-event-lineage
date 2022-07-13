/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * A connector primarily intended for system tests. The connector simply generates as many tasks as
 * requested. The tasks print metadata in the form of JSON to stdout for each message generated,
 * making externally visible which messages have been sent. Each message is also assigned a unique,
 * increasing seqno that is passed to Kafka Connect; when tasks are started on new nodes, this seqno
 * is used to resume where the task previously left off, allowing for testing of distributed Kafka
 * Connect.
 * <p>
 * If logging is left enabled, log output on stdout can be easily ignored by checking whether a
 * given line is valid JSON.
 */
public class VerifiableIndividuallyTracedSourcePairSendingTask extends
    VerifiableSourcePairSendingTask {

  public static final String SPAN_NAME = "test-source-process";


  @Override
  //Generate 2 message at a time for testing
  public List<SourceRecord> poll() throws InterruptedException {
    long sendStartMs = System.currentTimeMillis();
    if (throttler.shouldThrottle(seqno - startingSeqno, sendStartMs)) {
      throttler.throttle();
    }
    List<SourceRecord> result = new ArrayList<>();
    if (sent) {
      return result;
    }
    for (int i = 0; i < 2; i++) {
      //Simulating source task that has span created per individual record creation - for example ConsumerRecords iteration in Replicator.
      SpanBuilder spanBuilder = GlobalOpenTelemetry
          .getTracer("test-instrumentation")
          .spanBuilder(SPAN_NAME)
          .setParent(Context.current());
      Span span = spanBuilder.startSpan();
      try (Scope ignored = span.makeCurrent()) {
        Map<String, Long> ccOffset = Collections.singletonMap(SEQNO_FIELD, seqno);
        SourceRecord srcRecord = new SourceRecord(partition, ccOffset, topic,
            Schema.INT32_SCHEMA, id, Schema.INT64_SCHEMA, seqno);
        result.add(srcRecord);
        seqno++;
        span.end();
      }
    }
    sent = true;
    return result;
  }
}
