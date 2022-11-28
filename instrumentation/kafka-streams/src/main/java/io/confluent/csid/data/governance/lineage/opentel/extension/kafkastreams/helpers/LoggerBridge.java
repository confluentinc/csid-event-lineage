package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

@UtilityClass
@Slf4j
public class LoggerBridge {
  public static void log(Level level, String message,  Object... args) {
    switch (level){
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
