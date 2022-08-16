/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanSuppressionConfiguration;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Span filtering Sampler implementation
 * <p>
 * Wrapper Sampler - determines if Span should be suppressed based on span name and extension
 * configuration {@link SpanSuppressionConfiguration#SPAN_SUPPRESSION_BLACKLIST_PROP}.
 * <p>If Span is not configured for suppression by this Sampler - delegates Span sampling decision
 * down the chain to the wrapped Sampler.
 */
@Slf4j
public class ConnectSpanFilteringSampler implements Sampler {

  private final Sampler delegate;
  private final Set<String> blacklistedNames;

  public ConnectSpanFilteringSampler(Sampler delegate, Set<String> blacklist) {
    this.delegate = delegate;
    this.blacklistedNames = blacklist;
  }

  @Override
  public SamplingResult shouldSample(Context parentContext, String traceId, String name,
      SpanKind spanKind, Attributes attributes, List<LinkData> parentLinks) {
    if (!shouldSkip(parentContext, traceId, name, spanKind, attributes, parentLinks)) {
      return delegate.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
    } else {
      return SamplingResult.drop();
    }
  }

  private boolean shouldSkip(Context parentContext, String traceId, String name, SpanKind spanKind,
      Attributes attributes, List<LinkData> parentLinks) {
    return hasBlacklistedName(name);
  }

  private boolean hasBlacklistedName(String name) {
    boolean isBlacklisted = blacklistedNames.stream()
        .anyMatch(toIgnoreSpanName -> name.toLowerCase().startsWith(toIgnoreSpanName));
    log.debug("Span blacklisted={}, name={}, blacklistedSpans={}", isBlacklisted, name,
        blacklistedNames);
    return isBlacklisted;
  }


  @Override
  public String getDescription() {
    return "Sampler filtering out Connect platform internal operations based on span name blacklist";
  }
}
