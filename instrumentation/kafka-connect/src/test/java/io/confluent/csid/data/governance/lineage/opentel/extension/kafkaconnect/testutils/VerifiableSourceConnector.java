/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * Connector adapted for testing, based on VerifiableSourceConnector changed to be extendable for
 * ease of test variation.
 *
 * @see org.apache.kafka.connect.tools.VerifiableSinkConnector
 * @see VerifiableSourcePairSendingTask
 */
public class VerifiableSourceConnector extends SourceConnector {

  private Map<String, String> config;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return VerifiableSourcePairSendingTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> props = new HashMap<>(config);
      props.put(VerifiableSourcePairSendingTask.ID_CONFIG, String.valueOf(i));
      configs.add(props);
    }
    return configs;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }
}
