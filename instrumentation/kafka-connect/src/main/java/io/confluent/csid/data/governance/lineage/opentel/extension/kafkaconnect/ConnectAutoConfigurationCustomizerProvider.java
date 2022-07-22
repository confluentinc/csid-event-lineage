package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;


import com.google.auto.service.AutoService;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.DelegatingSpanExporter;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.InheritedAttributesSpanProcessor;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;

@AutoService(AutoConfigurationCustomizerProvider.class)
public class ConnectAutoConfigurationCustomizerProvider implements
    AutoConfigurationCustomizerProvider {

  @Override
  public void customize(AutoConfigurationCustomizer autoConfiguration) {
    autoConfiguration.addTracerProviderCustomizer(
        (sdkTracerProviderBuilder, configProperties) -> sdkTracerProviderBuilder.addSpanProcessor(
            new InheritedAttributesSpanProcessor()));
    autoConfiguration.addSpanExporterCustomizer(
        (spanExporter, configProperties) -> new DelegatingSpanExporter(spanExporter));
  }
}
