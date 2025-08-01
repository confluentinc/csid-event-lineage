/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.hasClassesNamed;
import static java.util.Arrays.asList;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import java.util.List;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.streams.state.internals.CachingStoreInstrumentation;
import org.apache.kafka.streams.state.internals.DirtyEntryInstrumentation;
import org.apache.kafka.streams.state.internals.KeyValueCachingStoreInstrumentation;
import org.apache.kafka.streams.state.internals.LRUCacheEntryInstrumentation;
import org.apache.kafka.streams.state.internals.WindowAndSessionCachingStoreInstrumentation;

/**
 * KafkaStreams instrumentation registration module specifies instrumentation, helper classes and
 * instrumentation application order
 */
@AutoService(InstrumentationModule.class)
public class KafkaStreamsInstrumentationModule extends InstrumentationModule {

  private final int OUTER_ORDER_OF_EXECUTION = -10;

  public KafkaStreamsInstrumentationModule() {
    super("kafka-streams-extension", "kafka-streams-2.6-extension");
  }

  /**
   * Instrumentation execution order - this extended Instrumentation should be executed after
   * OpenTelemetry agent kafka streams instrumentation, but since onExit advice is used - order is
   * reversed - hence negative order value is specified.
   * <p>
   * Order of execution is
   * <pre>
   * { instrumentation order -10 }     -- This extension instrumentation onEnter
   *    { instrumentation order 1 }       -- OpenTelemetry Kafka Streams instrumentation onEnter
   *        { actual class method }
   *    { instrumentation order 1 }       -- OpenTelemetry Kafka Streams instrumentation onExit
   * { instrumentation order -10 }     -- This extension instrumentation onExit
   * </pre>
   *
   * @return instrumentation execution order position
   */
  @Override
  public int order() {
    return OUTER_ORDER_OF_EXECUTION;
  }

  @Override
  public ElementMatcher.Junction<ClassLoader> classLoaderMatcher() {
    return hasClassesNamed("org.apache.kafka.streams.kstream.KStream");
  }

  @Override
  public List<TypeInstrumentation> typeInstrumentations() {
    return asList(
        new PartitionGroupInstrumentation(),
        new KeyValueStoreBuilderInstrumentation(),
        new WindowStoreBuilderInstrumentation(),
        new TimestampedBytesStoreInstrumentation(),
        new SessionStoreBuilderInstrumentation(),
        new ValueAndTimestampDeserializerInstrumentation(),
        new RecordConvertersInstrumentation(),
        new StreamTaskInstrumentation(),
        new LRUCacheEntryInstrumentation(),
        new CachingStoreInstrumentation(),
        new KeyValueCachingStoreInstrumentation(),
        new WindowAndSessionCachingStoreInstrumentation(),
        new DirtyEntryInstrumentation());
  }

  @Override
  public boolean isHelperClass(String className) {
    return className.startsWith(
        "io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon") ||
        className.startsWith(
            "io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers");
  }
}
