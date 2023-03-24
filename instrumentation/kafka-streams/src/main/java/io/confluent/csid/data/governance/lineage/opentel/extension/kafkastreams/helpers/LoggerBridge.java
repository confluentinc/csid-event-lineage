/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

/**
 * This class is a logging bridge between the OpenTelemetry Instrumentation classes and the SLF4J
 * API. We cannot use the SLF4J Loggers directly in the instrumentation advice classes - hence this
 * bridge.
 */
@UtilityClass
@Slf4j
public class LoggerBridge {

  public static void log(Level level, String message, Object... args) {
    switch (level) {
      case ERROR:
        log.error(message, args);
        break;
      case WARN:
        log.warn(message, args);
        break;
      case INFO:
        log.info(message, args);
        break;
      case DEBUG:
        log.debug(message, args);
        break;
      case TRACE:
        log.trace(message, args);
        break;
    }
  }
}
