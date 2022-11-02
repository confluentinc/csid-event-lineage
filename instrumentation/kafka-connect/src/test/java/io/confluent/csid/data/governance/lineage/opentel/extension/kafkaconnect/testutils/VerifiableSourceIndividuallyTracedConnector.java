/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import org.apache.kafka.connect.connector.Task;

/**
 * Extension of {@link VerifiableSourceConnector} - wraps / configures
 * {@link VerifiableIndividuallyTracedSourcePairSendingTask} - see task docs for more details
 *
 * @see VerifiableSourceConnector
 * @see VerifiableIndividuallyTracedSourcePairSendingTask
 */
public class VerifiableSourceIndividuallyTracedConnector extends VerifiableSourceConnector {

  @Override
  public Class<? extends Task> taskClass() {
    return VerifiableIndividuallyTracedSourcePairSendingTask.class;
  }
}
