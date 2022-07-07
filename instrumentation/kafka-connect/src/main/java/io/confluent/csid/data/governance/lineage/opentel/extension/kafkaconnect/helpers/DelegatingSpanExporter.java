package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SERVICE_NAME_KEY;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.DelegatingSpanData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Wraps configured {@link SpanExporter} with additional logic - for each Span prior to passing to
 * delegate SpanExporter - moves Service Name from Attributes to Resource Attributes - to override
 * Service name.
 * <p>
 * Captured Span Service name is recorded as Resource "service.name" attribute and its fixed on
 * startup - normally that reflects Application name, but for Platform type of application - like
 * Kafka Connect - it is preferable to capture specific Services / Tasks as separate Services.
 * <p>
 * For example in Kafka Connect - this overriding allows to capture Spans for each Connector as
 * separate services.
 */
public class DelegatingSpanExporter implements SpanExporter {

  private final SpanExporter delegate;

  public DelegatingSpanExporter(SpanExporter delegate) {
    this.delegate = delegate;
  }

  @Override
  public CompletableResultCode export(Collection<SpanData> spans) {
    return delegate.export(
        spans.stream().map(this::overrideServiceName).collect(Collectors.toList()));
  }

  private SpanData overrideServiceName(SpanData spanData) {
    return new ServiceNameOverridingSpanData(spanData);
  }

  @Override
  public CompletableResultCode flush() {
    return delegate.flush();
  }

  @Override
  public CompletableResultCode shutdown() {
    return delegate.shutdown();
  }

  @Override
  public void close() {
    delegate.close();
  }


  static class ServiceNameOverridingSpanData extends DelegatingSpanData {

    private final String serviceName;

    protected ServiceNameOverridingSpanData(SpanData delegate) {
      super(delegate);
      this.serviceName = delegate.getAttributes().get(SERVICE_NAME_KEY);
    }

    @Override
    public Resource getResource() {
      Resource resource = super.getResource();
      if (serviceName != null && serviceName.length() > 0) {
        resource = resource.toBuilder().put(SERVICE_NAME_KEY, serviceName).build();
      }
      return resource;
    }

    @Override
    public Attributes getAttributes() {
      return super.getAttributes().toBuilder().remove(SERVICE_NAME_KEY).build();
    }
  }
}

