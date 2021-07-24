package io.confluent.csid.data.governance.lineage.common.serializers;

import io.confluent.csid.data.governance.lineage.common.BlockDeserializer;

public class StringDeserializer extends BlockDeserializer<String, org.apache.kafka.common.serialization.StringDeserializer> {
    public StringDeserializer() {
        super(new org.apache.kafka.common.serialization.StringDeserializer());
    }
}
