/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ConfigurationReloader {

  public void reloadSpanSuppressionConfiguration(SpanSuppressionConfiguration spanSuppressionConfiguration){
    spanSuppressionConfiguration.reloadConfiguration();
  }

  public void reloadHeaderCaptureConfiguration(HeaderCaptureConfiguration headerCaptureConfiguration){
    headerCaptureConfiguration.reloadConfiguration();
  }

}
