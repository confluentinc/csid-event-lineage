/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanSuppressionConfiguration;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.Span;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class DistributedConnectSpanSuppressionSmokeTest extends IntegrationTestBase {

  private String testTopic = "connect-topic";
  private final String SOURCE_TASK_NAME = String.format(SpanNames.TASK_SPAN_NAME_FORMAT, testTopic,
      SpanNames.SOURCE_TASK);
  private final String SEND_TASK_NAME = String.format(SpanNames.PRODUCE_CONSUME_TASK_FORMAT,
      testTopic, SpanNames.PRODUCER_SEND);

  @BeforeEach
  void setup() {
    super.setup();
    Properties suppressionProperties = new Properties();
    suppressionProperties.setProperty(SpanSuppressionConfiguration.SPAN_SUPPRESSION_BLACKLIST_PROP,
        "connect-status,connect-configs");
    suppressionProperties.setProperty("otel.instrumentation.jetty.enabled", "false");
    suppressionProperties.setProperty("otel.instrumentation.servlet.enabled", "false");
    suppressionProperties.setProperty(
        "otel.instrumentation.common.experimental.suppress-controller-spans", "true");
    startDistributedConnectContainer(suppressionProperties);
  }

  @AfterEach
  void cleanup() {
    super.cleanup();
  }

  @SneakyThrows
  @Test
  void testConfiguredSpansAreSuppressedUsingDistributedConnectAndSourceTask() {
    submitConnector();
    commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
        testTopic, 5);
    Collection<ExportTraceServiceRequest> traces = traceAssertUtils.waitForTraces();
    assertThat(uniqueTraceSpanNames(traces)).containsExactlyInAnyOrder(SOURCE_TASK_NAME,
        SEND_TASK_NAME);
  }

  private Set<String> uniqueTraceSpanNames(Collection<ExportTraceServiceRequest> traces) {
    return traces.stream().flatMap(t -> t.getResourceSpansList().stream().flatMap(
        r -> r.getInstrumentationLibrarySpansList().stream()
            .flatMap(s -> s.getSpansList().stream().map(Span::getName)))).collect(
        Collectors.toSet());
  }

  private void submitConnector() {
    commonTestUtils.waitUntil("Submit connector", () -> {
      try {
        return sumbitConnectorInternal();
      } catch (IOException e) {
        log.warn("Failed to submit connector", e);
        return false;
      }
    });
  }

  private boolean sumbitConnectorInternal() throws IOException {
    String jsonRequest = "{"
        + "  \"name\": \"VerifiableSourceConnector1\","
        + "  \"config\": {"
        + "    \"connector.class\": \"org.apache.kafka.connect.tools.VerifiableSourceConnector\","
        + "    \"topic\": \"connect-topic\","
        + "    \"throughput\": \"1\""
        + "  }"
        + "}";
    try (Response response = OkHttpUtils.client().newCall(new Request.Builder().url(
                "http://localhost:" + connectContainer.getMappedPort(28382) + "/connectors")
            .post(RequestBody.create(MediaType.get("application/json"), jsonRequest)).build())
        .execute()) {
      return response.isSuccessful();
    }
  }
}
