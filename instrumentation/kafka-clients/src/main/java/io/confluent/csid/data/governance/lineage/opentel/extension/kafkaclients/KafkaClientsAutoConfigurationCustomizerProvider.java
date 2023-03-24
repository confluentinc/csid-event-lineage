/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;


import static java.util.Collections.emptyList;

import com.google.auto.service.AutoService;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.InheritedAttributesSpanProcessor;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import java.util.List;

/**
 * Configuration customizer for OpenTelemetry autoconfiguration
 * <p>
 * Allows to customize span processing pipeline by configuring custom span processors, exporters
 * etc.
 */
@AutoService(AutoConfigurationCustomizerProvider.class)
public class KafkaClientsAutoConfigurationCustomizerProvider implements
    AutoConfigurationCustomizerProvider {

  private final List<AttributeKey<String>> inheritAttributeNoOverwriteKeys =
      List.of(Constants.CLUSTER_ID_KEY);

  @Override
  public void customize(AutoConfigurationCustomizer autoConfiguration) {
    autoConfiguration.addTracerProviderCustomizer(
        (sdkTracerProviderBuilder, configProperties) -> sdkTracerProviderBuilder.addSpanProcessor(
            new InheritedAttributesSpanProcessor(emptyList(),
                inheritAttributeNoOverwriteKeys)));
  }
}
