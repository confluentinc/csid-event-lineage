/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;

/**
 * Handles Span creation.
 */
public class SpanHandler {

  private final OpenTelemetryWrapper openTelemetryWrapper;
  private final String instrumentationName;

  public SpanHandler(OpenTelemetryWrapper openTelemetryWrapper, String instrumentationName) {
    this.openTelemetryWrapper = openTelemetryWrapper;
    this.instrumentationName = instrumentationName;
  }

  public Span createAndStartSpanWithLink(String spanName, Context parent, SpanContext linkedSpan) {
    return createAndStartSpan(spanName, parent, linkedSpan);
  }


  public Span createAndStartSpan(String spanName, Context parent) {
    return createAndStartSpan(spanName, parent, null);
  }

  public void addEventToSpan(Span spanToAddTo, String eventName, Attributes eventAttributes) {
    spanToAddTo.addEvent(eventName, eventAttributes);
  }

  public void captureServiceNameToCurrentSpan(String serviceName) {
    if (serviceName != null && serviceName.length() > 0) {
      openTelemetryWrapper.currentSpan().setAttribute(Constants.SERVICE_NAME_KEY, serviceName);
    }
  }

  public void captureClusterIdToCurrentSpan(String clusterId) {
    if (clusterId != null && clusterId.length() > 0) {
      openTelemetryWrapper.currentSpan().setAttribute(Constants.CLUSTER_ID_KEY, clusterId);
    }
  }

  private Span createAndStartSpan(String spanName, Context parent, SpanContext linkedSpan) {
    SpanBuilder spanBuilder = openTelemetryWrapper.globalOpenTelemetry()
        .getTracer(instrumentationName)
        .spanBuilder(spanName)
        .setParent(parent);
    if (null != linkedSpan) {
      spanBuilder.addLink(linkedSpan);
    }
    return spanBuilder.startSpan();
  }

  public void captureServiceMetadataToSpan(ServiceMetadata serviceMetadata) {
    if (serviceMetadata != null) {
      captureServiceNameToCurrentSpan(serviceMetadata.getServiceName());
      captureClusterIdToCurrentSpan(serviceMetadata.getClusterId());
    }
  }
}
