/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.spanSuppressionConfiguration;

import com.google.auto.service.AutoService;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.InheritedAttributesSpanProcessor;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.ConnectSpanFilteringSampler;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.DelegatingSpanExporter;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import java.util.List;
import java.util.Set;

/**
 * Configuration customizer for OpenTelemetry autoconfiguration
 * <p>
 * Allows to customize span processing pipeline by configuring custom span processors, exporters
 * etc.
 */
@AutoService(AutoConfigurationCustomizerProvider.class)
public class ConnectAutoConfigurationCustomizerProvider implements
    AutoConfigurationCustomizerProvider {

  private final List<AttributeKey<String>> inheritAttributeKeys =
      List.of(Constants.SERVICE_NAME_KEY);

  private final List<AttributeKey<String>> inheritAttributeNoOverwriteKeys =
      List.of(Constants.CLUSTER_ID_KEY);

  @Override
  public void customize(AutoConfigurationCustomizer autoConfiguration) {
    autoConfiguration.addTracerProviderCustomizer(
        (sdkTracerProviderBuilder, configProperties) -> sdkTracerProviderBuilder.addSpanProcessor(
            new InheritedAttributesSpanProcessor(inheritAttributeKeys,
                inheritAttributeNoOverwriteKeys)));
    autoConfiguration.addSpanExporterCustomizer(
        (spanExporter, configProperties) -> new DelegatingSpanExporter(spanExporter));

    Set<String> spanSuppressionBlacklist = spanSuppressionConfiguration().getSpanSuppressionBlacklist();
    if (!spanSuppressionBlacklist.isEmpty()) {
      autoConfiguration.addSamplerCustomizer(
          (sampler, configProperties) -> new ConnectSpanFilteringSampler(sampler,
              spanSuppressionBlacklist));
    }
  }
}
