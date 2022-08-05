package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
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
import lombok.RequiredArgsConstructor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.tuple.Pair;

@RequiredArgsConstructor
public class TraceAssertUtils {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final OkHttpClient client = OkHttpUtils.client();

  private final String backendHost;
  private final Integer backendPort;


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
                throw new RuntimeException(e);
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
              .url(String.format("http://%s:%d/get-traces", backendHost,
                  backendPort))
              .build();

      try (Response response = client.newCall(request).execute()) {
        if (response.body() != null) {
          content = response.body().string();
        } else {
          content = "[]";
        }
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

  static String spanAttributeValue(Span span, String attributeKey) {
    return span.getAttributesList().stream().filter(attr -> attr.getKey().equals(attributeKey))
        .findAny().map(kv -> kv.getValue().getStringValue()).orElse(null);
  }

  static void assertSpanAttribute(Span span, String attributeKey, String attributeValue) {
    assertThat(
        spanAttributeValue(span, attributeKey)).as(
            "Assertion failed for Span header key=%s, spanName=%s, spanId=%s", attributeKey,
            span.getName(),
            span.getSpanId().toStringUtf8())
        .isEqualTo(attributeValue);
  }
}
