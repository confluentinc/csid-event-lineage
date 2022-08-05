/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils.DOCKER_NETWORK;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
  private static final String CONNECT_TEMP_DIR = "/tmp";
  private static final Integer COLLECTOR_CONTAINER_PORT = 8080;
  protected static final String extensionPath =
      System.getProperty("io.opentelemetry.smoketest.extensionPath");

  private GenericContainer backend;

  private GenericContainer<?> connectContainer;

  protected String testTopic = "connect-topic";

  protected final String transformClassName = "InsertHeaderBytes";

  protected CommonTestUtils commonTestUtils;

  protected TraceAssertUtils traceAssertUtils;

  void setup() {
    startCollector();
    traceAssertUtils = new TraceAssertUtils(backend.getHost(), backend.getMappedPort(COLLECTOR_CONTAINER_PORT));
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
            .withExposedPorts(COLLECTOR_CONTAINER_PORT)
            .waitingFor(Wait.forHttp("/health").forPort(COLLECTOR_CONTAINER_PORT))
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
        .withCopyFileToContainer(MountableFile.forClasspathResource("runscript", 0777),
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
}
