/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.hasClassesNamed;
import static java.util.Arrays.asList;

import com.google.auto.service.AutoService;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import java.util.List;
import net.bytebuddy.matcher.ElementMatcher;

/**
 * Kafka Connect instrumentation registration module.
 * <p>
 * Specifies instrumentation and helper classes and instrumentation application order
 */
@AutoService(InstrumentationModule.class)
public final class KafkaConnectInstrumentationModule extends InstrumentationModule {

  private final int ORDER_OF_EXECUTION = 10;

  public KafkaConnectInstrumentationModule() {
    super("kafka-connect-extension", "kafka-connect-2.6-extension");
  }

  /**
   * Instrumentation execution order - allows specifying additional instrumentation executed before
   * this one (with lower order) or after (with higher order).
   *
   * @return instrumentation execution order position
   */
  @Override
  public int order() {
    return ORDER_OF_EXECUTION;
  }

  @Override
  public ElementMatcher.Junction<ClassLoader> classLoaderMatcher() {
    return hasClassesNamed("org.apache.kafka.connect.runtime.Connect");
  }

  @Override
  public List<TypeInstrumentation> typeInstrumentations() {
    return asList(
        new ConnectTransformationInstrumentation(),
        new ConnectSinkTaskInstrumentation(),
        new ConnectWorkerSourceTaskInstrumentation(),
        new ConnectSourceRecordInstrumentation()
    );
  }

  @Override
  public boolean isHelperClass(String className) {
    return Singletons.isHelperClass(className);
  }
}
