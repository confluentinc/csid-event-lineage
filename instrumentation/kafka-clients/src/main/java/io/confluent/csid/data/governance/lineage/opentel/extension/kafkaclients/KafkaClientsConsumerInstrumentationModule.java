/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.hasClassesNamed;
import static java.util.Arrays.asList;

import com.google.auto.service.AutoService;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import java.util.List;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * {@link KafkaConsumer} instrumentation registration module.
 * <p>
 * Specifies instrumentation and helper classes and instrumentation execution order
 */
@AutoService(InstrumentationModule.class)
public final class KafkaClientsConsumerInstrumentationModule extends InstrumentationModule {

  private final int OUTER_ORDER_OF_EXECUTION = -10;

  public KafkaClientsConsumerInstrumentationModule() {
    super("kafka-clients-extension", "kafka-clients-2.6-extension");
  }

  /**
   * Instrumentation execution order - this extended Instrumentation should be executed after
   * OpenTelemetry agent kafka client instrumentation, but since onExit advice is used - order is
   * reversed and negative order value is specified.
   * <p>
   * Order of execution is
   * <pre>
   * { instrumentation order -10 }     -- This extension instrumentation onEnter
   *    { instrumentation order 1 }       -- OpenTelemetry Kafka Client instrumentation onEnter
   *        { actual class method }           -- Actual class method
   *    { instrumentation order 1 }       -- OpenTelemetry Kafka Client instrumentation onExit
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
    return hasClassesNamed("org.apache.kafka.clients.consumer.KafkaConsumer");
  }

  @Override
  public List<TypeInstrumentation> typeInstrumentations() {
    return asList(
        new ExtendedKafkaConsumerRecordIteratorInstrumentation()
    );
  }

  @Override
  public boolean isHelperClass(String className) {
    return Singletons.isHelperClass(className);
  }
}
