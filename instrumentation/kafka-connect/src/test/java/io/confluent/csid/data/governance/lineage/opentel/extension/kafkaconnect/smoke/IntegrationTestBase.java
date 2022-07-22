/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SERVICE_NAME_KEY;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils.DOCKER_NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

@Slf4j
abstract class IntegrationTestBase {

  public static class Connectors {

    public static String SOURCE_NO_SMT = "source_no_smt.properties";
    public static String SINK_NO_SMT = "sink_no_smt.properties";
    public static String SOURCE_WITH_SMT = "source_with_smt.properties";
    public static String SINK_WITH_SMT = "sink_with_smt.properties";

    public static String SINK_CONNECTOR_NAME = "VerifiableSinkTask1";
    public static String SOURCE_CONNECTOR_NAME = "VerifiableSourceTask1";
  }


  private static final String KAFKA_CONTAINER_VERSION = "7.0.1";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String CONNECT_TEMP_DIR = "/tmp";

  protected static OkHttpClient client = OkHttpUtils.client();

  protected static final String extensionPath =
      System.getProperty("io.opentelemetry.smoketest.extensionPath");

  private GenericContainer backend;

  private GenericContainer<?> connectContainer;

  protected String testTopic = "connect-topic";

  protected final String transformClassName = "InsertHeaderBytes";

  protected CommonTestUtils commonTestUtils;

  void setup() {
    startCollector();
    commonTestUtils = new CommonTestUtils(CONNECT_TEMP_DIR);
    commonTestUtils.startKafkaContainer();
  }

  void cleanup() {
    stopConnectContainer();
    stopCollectorContainer();
    commonTestUtils.stopKafkaContainer();
  }

  private void startCollector() {
    backend =
        new GenericContainer<>(
            "ghcr.io/open-telemetry/opentelemetry-java-instrumentation/smoke-test-fake-backend:20210918.1248928123")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/health").forPort(8080))
            .withNetwork(DOCKER_NETWORK)
            .withNetworkAliases("backend")
            .withLogConsumer(new Slf4jLogConsumer(log));
    backend.start();
  }


  @SneakyThrows
  void startConnectContainer(String... connectors) {
    connectContainer = buildConnectContainer(connectors);
    connectContainer.start();
  }

  private GenericContainer<?> buildConnectContainer(
      String... connectors) {

    return new GenericContainer<>("confluentinc/cp-kafka-connect:" + KAFKA_CONTAINER_VERSION)
        //.withExposedPorts(28382)
        .withNetwork(DOCKER_NETWORK)
        .withNetworkAliases("connect")
        .withLogConsumer(new Slf4jLogConsumer(log))
        .withCopyFileToContainer(MountableFile.forClasspathResource("runscript", 777),
            "/usr/share/runscript")
        .withCopyFileToContainer(MountableFile.forClasspathResource("launch"),
            "/usr/share/launch")
        .withCopyFileToContainer(MountableFile.forClasspathResource("source_no_smt.properties"),
            "/usr/share/props/source_no_smt.properties")
        .withCopyFileToContainer(MountableFile.forClasspathResource("sink_no_smt.properties"),
            "/usr/share/props/sink_no_smt.properties")
        .withCopyFileToContainer(MountableFile.forClasspathResource("sink_with_smt.properties"),
            "/usr/share/props/sink_with_smt.properties")
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("source_with_smt.properties"),
            "/usr/share/props/source_with_smt.properties")
        .withCopyFileToContainer(MountableFile.forClasspathResource("standalone.properties"),
            "/usr/share/props/standalone.properties")
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("insertHeaderBytes-1.0-SNAPSHOT.jarfile"),
            "/etc/kafka-connect/jars/insertHeaderBytes-1.0-SNAPSHOT.jar")
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("opentelemetry-javaagent-1.13.0.jarfile"),
            "/opt/opentelemetry-javaagent.jar")
        .withCopyFileToContainer(
            MountableFile.forHostPath(extensionPath),
            "/opt/opentelemetry-extensions.jar")
        // Adds instrumentation agent with debug configuration to the target application
        .withEnv(
            "JAVA_TOOL_OPTIONS",
            "-javaagent:/opt/opentelemetry-javaagent.jar "
                + "-Dotel.javaagent.debug=true "
                + "-Dotel.javaagent.testing.additional-library-ignores.enabled=false "
                + "-Dotel.javaagent.testing.fail-on-context-leak=true "
                + "-Dotel.metrics.exporter=none "
                + "-Dotel.exporter.otlp.endpoint=http://backend:8080/ "
                + "-Devent.lineage.header-capture-whitelist=captured_propagated_header,captured_propagated_header2 "
                + "-Devent.lineage.header-propagation-whitelist=captured_propagated_header,captured_propagated_header2 "
                + "-Devent.lineage.header-charset=UTF-8")
        .withEnv("OTEL_JAVAAGENT_EXTENSIONS", "/opt/opentelemetry-extensions.jar")
        .withEnv("OTEL_BSP_MAX_EXPORT_BATCH", "1")
        .withEnv("OTEL_BSP_SCHEDULE_DELAY", "10")
        .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:9092")
        .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
        .withEnv("CONNECT_LISTENERS", "http://0.0.0.0:28382")
        .withEnv("CONNECT_GROUP_ID", "connect")
        .withEnv("CONNECT_PRODUCER_CLIENT_ID", "connect-worker-producer")
        .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs")
        .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
        .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "10000")
        .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets")
        .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
        .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status")
        .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
        .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
        .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
        .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java")
        .withEnv("CONNECT_LOG4J_LOGGERS",
            "org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR")
        .withEnv("CONNECTORS", String.join(" ", connectors))
        .withCommand("sh", "/usr/share/runscript")
        .waitingFor(Wait.forLogMessage(".*Kafka Connect started.*", 1));
  }

  private void stopConnectContainer() {
    connectContainer.stop();
  }

  private void stopCollectorContainer() {
    backend.stop();
  }

  protected Collection<ExportTraceServiceRequest> waitForTraces()
      throws IOException, InterruptedException {
    String content = waitForContent();

    return StreamSupport.stream(OBJECT_MAPPER.readTree(content).spliterator(), false)
        .map(
            it -> {
              ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
              try {
                JsonFormat.parser().merge(OBJECT_MAPPER.writeValueAsString(it), builder);
              } catch (InvalidProtocolBufferException | JsonProcessingException e) {
                e.printStackTrace();
              }
              return builder.build();
            })
        .collect(Collectors.toList());
  }

  private String waitForContent() throws IOException, InterruptedException {
    long previousSize = 0;
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2);
    String content = "[]";
    while (System.currentTimeMillis() < deadline) {

      Request request =
          new Request.Builder()
              .url(String.format("http://%s:%d/get-traces", backend.getHost(),
                  backend.getMappedPort(8080)))
              .build();

      try (ResponseBody body = client.newCall(request).execute().body()) {
        content = body.string();
      }

      if (content.length() > 2 && content.length() == previousSize) {
        break;
      }
      previousSize = content.length();
      System.out.printf("Current content size %d%n", previousSize);
      TimeUnit.MILLISECONDS.sleep(500);
    }

    return content;
  }

  List<Pair<Resource, Span>> findTracesWithSpanNames(
      Map<String, List<Pair<Resource, Span>>> groupedTraces, String... spanNames) {
    return groupedTraces.entrySet().stream().filter(entry -> Arrays.stream(spanNames).allMatch(
            spanName -> entry.getValue().stream().anyMatch(
                resourceSpanPair -> resourceSpanPair.getRight().getName().contains(spanName))))
        .findFirst().map(
            Entry::getValue).orElse(null);
  }

  List<Pair<Resource, Span>> filterSpansBySpanNames(
      List<Pair<Resource, Span>> spans, String... spanNames) {
    return spans.stream().filter(resourceSpanPair -> Arrays.stream(spanNames).anyMatch(
        spanName -> resourceSpanPair.getRight().getName().contains(spanName))).collect(
        Collectors.toList());
  }

  Map<String, List<Pair<Resource, Span>>> groupByTrace(
      Collection<ExportTraceServiceRequest> traces) {
    Map<String, List<Pair<Resource, Span>>> grouped = new HashMap<>();
    List<Pair<Resource, Span>> spans = traces.stream().flatMap(
            traceData -> traceData.getResourceSpansList().stream().flatMap(
                r -> r.getInstrumentationLibrarySpansList().stream()
                    .flatMap(i -> i.getSpansList().stream().map(s -> Pair.of(r.getResource(), s)))))
        .collect(
            Collectors.toList());
    spans.forEach(resourceSpanPair -> {
      List<Pair<Resource, Span>> pairList;
      if (grouped.containsKey(resourceSpanPair.getRight().getTraceId().toStringUtf8())) {
        pairList = grouped.get(resourceSpanPair.getRight().getTraceId().toStringUtf8());
      } else {
        pairList = new ArrayList<>();
      }
      pairList.add(resourceSpanPair);
      grouped.put(resourceSpanPair.getRight().getTraceId().toStringUtf8(), pairList);

    });
    return grouped;
  }

  List<Pair<Resource, Span>> findTraceBySpanNamesWithinTimeout(int secondsToWait,
      String... spanNames) {
    List<Pair<Resource, Span>> expectedTrace = new ArrayList<>();
    await().alias("Could not find trace with " + String.join(", ", spanNames) + " spans")
        .atMost(Duration.ofSeconds(secondsToWait)).until(() -> {
          Collection<ExportTraceServiceRequest> traces = waitForTraces();

          Map<String, List<Pair<Resource, Span>>> groupedTraces = groupByTrace(traces);
          List<Pair<Resource, Span>> filteredTrace = findTracesWithSpanNames(groupedTraces,
              spanNames);
          if (filteredTrace != null) {
            expectedTrace.addAll(filteredTrace);
            return true;
          }
          return false;
        });
    return expectedTrace;
  }

  String attributeValue(List<KeyValue> attributeList, String attributeKey) {
    return attributeList.stream().filter(attr -> attr.getKey().equals(attributeKey))
        .findAny().map(kv -> kv.getValue().getStringValue()).orElse(null);
  }

  void assertSpanAttribute(Span span, String attributeKey, String attributeValue) {
    assertThat(
        attributeValue(span.getAttributesList(), attributeKey)).as(
            "Assertion failed for Span header key=%s, spanName=%s, spanId=%s", attributeKey,
            span.getName(),
            span.getSpanId().toStringUtf8())
        .isEqualTo(attributeValue);
  }

  protected void assertServiceName(Pair<Resource, Span> resourceSpanPair,
      String expectedServiceName) {
    String serviceNameResourceAttributeValue = attributeValue(
        resourceSpanPair.getLeft().getAttributesList(), SERVICE_NAME_KEY.getKey());
    assertThat(serviceNameResourceAttributeValue).isNotNull();
    assertThat(serviceNameResourceAttributeValue).as(
            "Unexpected service.name resource attribute value of %s, for trace %s",
            serviceNameResourceAttributeValue, resourceSpanPair.toString())
        .isEqualTo(expectedServiceName);
  }
}
