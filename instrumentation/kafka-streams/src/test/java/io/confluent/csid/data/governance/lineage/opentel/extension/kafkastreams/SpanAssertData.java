/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.HEADER_ATTRIBUTE_PREFIX;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_CACHE_PUT;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_CACHE_REMOVE;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_DELETE;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_FLUSH;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_GET;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_PUT;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_REMOVE;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.assertj.SpanDataAssert;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.common.header.Header;

public class SpanAssertData implements BiConsumer<SpanDataAssert, SpanData> {

  Consumer<SpanDataAssert> assertions;
  SpanData actual;

  Supplier<String> spanDescriptionSupplier = () -> String.format(
      "Span name %s, span id %s, trace id %s", actual.getName(), actual.getSpanId(),
      actual.getTraceId());

  @Override
  public void accept(SpanDataAssert spanDataAssert, SpanData actual) {
    this.actual = actual;
    this.assertions.accept(spanDataAssert);
  }

  static SpanAssertData produce() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert -> spanAssert.hasKind(SpanKind.PRODUCER);
    return spanAssertData;
  }

  static SpanAssertData consume() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert -> spanAssert.hasKind(SpanKind.CONSUMER);
    return spanAssertData;
  }

  static SpanAssertData stateStoreGet() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_GET));
    return spanAssertData;
  }

  static SpanAssertData stateStorePut() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_PUT));
    return spanAssertData;
  }

  static SpanAssertData stateStoreCacheRemove() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_CACHE_REMOVE));
    return spanAssertData;
  }

  static SpanAssertData stateStoreCachePut() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_CACHE_PUT));
    return spanAssertData;
  }

  static SpanAssertData stateStoreFlush() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_FLUSH));
    return spanAssertData;
  }

  static SpanAssertData stateStoreDelete() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_DELETE));
    return spanAssertData;
  }

  static SpanAssertData stateStoreRemove() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_REMOVE));
    return spanAssertData;
  }

  static SpanAssertData produceChangelog() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.PRODUCER)
            .satisfies(spanData -> assertThat(spanData.getName()).contains("changelog"));
    return spanAssertData;
  }

  SpanAssertData withHeaders(Charset charset, Header... expectedHeaders) {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.as(spanDescriptionSupplier).hasAttributesSatisfying(attributes ->
            Arrays.stream(expectedHeaders).forEach(header ->
                assertThat(attributes.get(headerKey(header.key()))).as(
                    "Header %s is not equal for span assertion %s", headerKey(header.key()),
                    spanAssert.info).isEqualTo(
                    new String(header.value(), charset)))));
    return this;
  }

  SpanAssertData withoutHeaders(String... notExpectedHeaderKeys) {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.as(spanDescriptionSupplier).hasAttributesSatisfying(attributes ->
            Arrays.stream(notExpectedHeaderKeys).forEach(headerKey ->
                assertThat(attributes.get(headerKey(headerKey))).isNull())));
    return this;
  }

  SpanAssertData withoutHeaders(Header... notExpectedHeaders) {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.as(spanDescriptionSupplier).hasAttributesSatisfying(attributes ->
            Arrays.stream(notExpectedHeaders).forEach(header ->
                assertThat(attributes.get(headerKey(header.key()))).isNull())));
    return this;
  }

  SpanAssertData withLink() {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.as(spanDescriptionSupplier).hasTotalRecordedLinks(1));
    return this;
  }

  SpanAssertData withNameContaining(String containing) {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.as(spanDescriptionSupplier)
            .satisfies(
                spanData -> assertThat(spanData.getName()).contains(containing)));
    return this;
  }

  private static AttributeKey<String> headerKey(String headerKey) {
    return stringKey(HEADER_ATTRIBUTE_PREFIX + headerKey);
  }
}
