/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_HEADER;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StringTextMapSetterTest {

  StringTextMapSetter classUnderTest = new StringTextMapSetter();

  @Test
  void setsValueToCarriedWhenKeyIsTracingHeaderKey() {
    String[] carrier = new String[1];
    String key = TRACING_HEADER;
    String value = "TraceIdToGEt";

    classUnderTest.set(carrier, key, value);
    assertThat(carrier).containsExactly(value);
  }

}