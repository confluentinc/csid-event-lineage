/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_HEADER;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StringTextMapGetterTest {

  StringTextMapGetter classUnderTest = new StringTextMapGetter();

  @Test
  void keys() {
    String carrier = "carrier";
    assertThat(classUnderTest.keys(carrier)).containsExactly(TRACING_HEADER);
  }

  @Test
  void getReturnsCarrierForTracingHeaderKey() {
    String carrier = "carrier";
    String key = TRACING_HEADER;
    assertThat(classUnderTest.get(carrier, key)).isEqualTo(carrier);
  }

  @Test
  void getReturnsNullForNotTracingHeaderKey() {
    String carrier = "carrier";
    String key = "Bad key";
    assertThat(classUnderTest.get(carrier, key)).isNull();

  }
}