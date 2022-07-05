/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.HEADER_ATTRIBUTE_PREFIX;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_DELETE;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_GET;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames.STATE_STORE_PUT;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.assertj.SpanDataAssert;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.function.Consumer;
import org.apache.kafka.common.header.Header;

public class SpanAssertData implements Consumer<SpanDataAssert> {

  Consumer<SpanDataAssert> assertions;

  @Override
  public void accept(SpanDataAssert spanDataAssert) {
    this.assertions.accept(spanDataAssert);
  }

  public static SpanAssertData produce() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert -> spanAssert.hasKind(SpanKind.PRODUCER);
    return spanAssertData;
  }

  public static SpanAssertData consume() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert -> spanAssert.hasKind(SpanKind.CONSUMER);
    return spanAssertData;
  }

  public static SpanAssertData stateStoreGet() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_GET));
    return spanAssertData;
  }

  public static SpanAssertData smt() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(SpanNames.SMT));
    return spanAssertData;
  }

  public static SpanAssertData sourceTask() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(SpanNames.SOURCE_TASK));
    return spanAssertData;
  }

  public static SpanAssertData sinkTask() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(SpanNames.SINK_TASK));
    return spanAssertData;
  }
  public static SpanAssertData stateStorePut() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_PUT));
    return spanAssertData;
  }

  public static SpanAssertData stateStoreDelete() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.INTERNAL)
            .satisfies(spanData -> assertThat(spanData.getName()).contains(STATE_STORE_DELETE));
    return spanAssertData;
  }

  public static SpanAssertData produceChangelog() {
    SpanAssertData spanAssertData = new SpanAssertData();
    spanAssertData.assertions = spanAssert ->
        spanAssert
            .hasKind(SpanKind.PRODUCER)
            .satisfies(spanData -> assertThat(spanData.getName()).contains("changelog"));
    return spanAssertData;
  }

  public SpanAssertData withHeaders(Charset charset, Header... expectedHeaders) {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.hasAttributesSatisfying(attributes ->
            Arrays.stream(expectedHeaders).forEach(header ->
                assertThat(attributes.get(headerKey(header.key()))).isEqualTo(
                    new String(header.value(), charset)))));
    return this;
  }

  public SpanAssertData withoutHeaders(String... notExpectedHeaderKeys) {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.hasAttributesSatisfying(attributes ->
            Arrays.stream(notExpectedHeaderKeys).forEach(headerKey ->
                assertThat(attributes.get(headerKey(headerKey))).isNull())));
    return this;
  }

  public SpanAssertData withoutHeaders(Header... notExpectedHeaders) {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.hasAttributesSatisfying(attributes ->
            Arrays.stream(notExpectedHeaders).forEach(header ->
                assertThat(attributes.get(headerKey(header.key()))).isNull())));
    return this;
  }

  public SpanAssertData withLink() {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.hasTotalRecordedLinks(1));
    return this;
  }

  public SpanAssertData withNameContaining(String containing) {
    this.assertions = this.assertions.andThen(
        spanAssert -> spanAssert.satisfies(
            spanData -> assertThat(spanData.getName()).contains(containing)));
    return this;
  }

  private static AttributeKey<String> headerKey(String headerKey) {
    return stringKey(HEADER_ATTRIBUTE_PREFIX + headerKey);
  }
}
