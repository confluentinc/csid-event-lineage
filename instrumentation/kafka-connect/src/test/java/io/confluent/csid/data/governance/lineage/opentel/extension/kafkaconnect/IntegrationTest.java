package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.OkHttpUtils;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Slf4j
class IntegrationTest {

  public static class Connectors {

    public static String SOURCE_NO_SMT = "source_no_smt.properties";
    public static String SINK_NO_SMT = "sink_no_smt.properties";
    public static String SOURCE_WITH_SMT = "source_with_smt.properties";
    public static String SINK_WITH_SMT = "sink_with_smt.properties";

  }


  private static final String KAFKA_CONTAINER_VERSION = "7.0.1";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected static OkHttpClient client = OkHttpUtils.client();

  protected static final String agentPath =
      "/Users/rkolesnev/code/rk-data-lineage/opentelemetry-javaagent-1.13.0.jar";
  // System.getProperty("io.opentelemetry.smoketest.agentPath");
  // Javaagent with extensions embedded inside it
  protected static final String extendedAgentPath =
      System.getProperty("io.opentelemetry.smoketest.extendedAgentPath");
  protected static final String extensionPath =
      System.getProperty("io.opentelemetry.smoketest.extensionPath");

  private GenericContainer backend;

  public static final Network DOCKER_NETWORK = Network.newNetwork();


  private String kafkaBootstrapServers = "dummy";
  private KafkaContainer kafkaContainer;

  private GenericContainer<?> connectContainer;

  protected String testTopic = "connect-topic";

  protected final String transformClassName = "InsertHeaderBytes";


  void setup() {
    startCollector();
    startKafkaContainer();
  }

  void cleanup() {
    stopConnectContainer();
    stopCollectorContainer();
    stopKafkaContainer();
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


  public void produceSingleEvent(String topic, String key, String value, Header... headers) {
    produceSingleEvent(topic, key, value, headers, StringSerializer.class, StringSerializer.class);
  }

  public void produceSingleEvent(String topic, Integer key, Integer value, Header... headers) {
    produceSingleEvent(topic, key, value, headers, IntegerSerializer.class,
        IntegerSerializer.class);
  }

  public void produceSingleEvent(
      String topic, Object key, Object value, Header[] headers, final Class<?> keySerializerClass,
      final Class<?> valueSerializerClass) {
    Properties overrides = new Properties();
    overrides.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
    overrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);

    KafkaProducer kafkaProducer = new KafkaProducer<>(
        getKafkaProperties(overrides));
    ProducerRecord producerRecord = new ProducerRecord<>(topic, key, value);
    Arrays.stream(headers).forEach(header -> producerRecord.headers().add(header));

    kafkaProducer.send(producerRecord);
    kafkaProducer.flush();
    kafkaProducer.close();
    log.info("Produced Records: {}", producerRecord);
  }

  public List<ConsumerRecord> consumeAtLeastXEvents(final Class<?> keyDeserializerClass,
      final Class<?> valueDeserializerClass, String topic, int minimumNumberOfEventsToConsume) {
    Properties overrides = new Properties();
    return consumeAtLeastXEvents(keyDeserializerClass, valueDeserializerClass, topic,
        minimumNumberOfEventsToConsume, overrides);
  }

  public List<ConsumerRecord> consumeAtLeastXEvents(final Class<?> keyDeserializerClass,
      final Class<?> valueDeserializerClass, String topic, int minimumNumberOfEventsToConsume,
      Properties overrides) {

    List<ConsumerRecord> consumed = new ArrayList<>();
    try {
      overrides.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
      overrides.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
      KafkaConsumer consumer = new KafkaConsumer(getKafkaProperties(overrides));
      consumer.subscribe(singleton(topic));
      await().atMost(Duration.ofSeconds(300)).pollInterval(Duration.ofMillis(200)).until(() -> {
        ConsumerRecords records = consumer.poll(Duration.ofMillis(200));
        Iterator<ConsumerRecord> recordIterator = records.iterator();
        while (recordIterator.hasNext()) {
          consumed.add(recordIterator.next());
        }
        return consumed.size() >= minimumNumberOfEventsToConsume;
      });
      consumer.commitSync();
      consumer.close();
    } catch (Exception e) {
      log.info("Consumed Records: {}",
          consumed.stream().map(event -> Pair.of(event.key(), event.value())).collect(
              Collectors.toList()));
      throw e;
    }
    log.info("Consumed Records: {}",
        consumed.stream().map(event -> Pair.of(event.key(), event.value())).collect(
            Collectors.toList()));
    return consumed;
  }

  public ConsumerRecord consumeEvent(final Class<?> keyDeserializerClass,
      final Class<?> valueDeserializerClass, String topic) {
    return consumeAtLeastXEvents(keyDeserializerClass, valueDeserializerClass, topic, 1).get(0);
  }

  private void startKafkaContainer() {
    if (kafkaContainer == null) {
      kafkaContainer = new KafkaContainer(
          DockerImageName.parse("confluentinc/cp-kafka:" + KAFKA_CONTAINER_VERSION))
          .withNetworkAliases("kafka")
          .withNetwork(DOCKER_NETWORK)
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500").withReuse(false);
    }
    kafkaContainer.start();
    kafkaBootstrapServers = kafkaContainer.getBootstrapServers();
  }

  private void stopKafkaContainer() {
    if (kafkaContainer != null) {
      kafkaContainer.stop();
    }
  }

  private Properties getKafkaProperties(Properties overrides) {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer-" + UUID.randomUUID());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
    props.putAll(overrides);
    return props;
  }

  private GenericContainer<?> buildConnectContainer(
      String... connectors) {
    GenericContainer<?> result =
        new GenericContainer<>("confluentinc/cp-kafka-connect:" + KAFKA_CONTAINER_VERSION)
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

    return result;
  }

  private void stopConnectContainer() {
    connectContainer.stop();
  }

  private void stopCollectorContainer() {
    backend.stop();
  }

  protected static int countResourcesByValue(
      Collection<ExportTraceServiceRequest> traces, String resourceName, String value) {
    return (int)
        traces.stream()
            .flatMap(it -> it.getResourceSpansList().stream())
            .flatMap(it -> it.getResource().getAttributesList().stream())
            .filter(
                kv ->
                    kv.getKey().equals(resourceName)
                        && kv.getValue().getStringValue().equals(value))
            .count();
  }

  protected static int countSpansByName(
      Collection<ExportTraceServiceRequest> traces, String spanName) {
    return (int) getSpanStream(traces).filter(it -> it.getName().equals(spanName)).count();
  }

  protected static int countSpansByAttributeValue(
      Collection<ExportTraceServiceRequest> traces, String attributeName, String attributeValue) {
    return (int)
        getSpanStream(traces)
            .flatMap(it -> it.getAttributesList().stream())
            .filter(
                kv ->
                    kv.getKey().equals(attributeName)
                        && kv.getValue().getStringValue().equals(attributeValue))
            .count();
  }

  protected static Stream<Span> getSpanStream(Collection<ExportTraceServiceRequest> traces) {
    return traces.stream()
        .flatMap(it -> it.getResourceSpansList().stream())
        .flatMap(it -> it.getInstrumentationLibrarySpansList().stream())
        .flatMap(it -> it.getSpansList().stream());
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

  String spanAttributeValue(Span span, String attributeKey) {
    return span.getAttributesList().stream().filter(attr -> attr.getKey().equals(attributeKey))
        .findAny().map(kv -> kv.getValue().getStringValue()).orElse(null);
  }

  void assertSpanAttribute(Span span, String attributeKey, String attributeValue) {
    assertThat(
        spanAttributeValue(span, attributeKey)).as(
            "Assertion failed for Span header key=%s, spanName=%s, spanId=%s", attributeKey,
            span.getName(),
            span.getSpanId().toStringUtf8())
        .isEqualTo(attributeValue);
  }
}
