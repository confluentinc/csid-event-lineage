/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import org.apache.kafka.connect.connector.Task;

/**
 * Extension of {@link VerifiableSourceConnector} - wraps / configures
 * {@link VerifiableBatchTracedSourcePairSendingTask} - see task docs for more details
 *
 * @see VerifiableSourceConnector
 * @see VerifiableBatchTracedSourcePairSendingTask
 */
public class VerifiableSourceBatchTracedConnector extends VerifiableSourceConnector {

  @Override
  public Class<? extends Task> taskClass() {
    return VerifiableBatchTracedSourcePairSendingTask.class;
  }
}
