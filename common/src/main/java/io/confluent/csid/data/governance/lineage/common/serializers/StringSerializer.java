package io.confluent.csid.data.governance.lineage.common.serializers;

import io.confluent.csid.data.governance.lineage.common.BlockSerializer;

public class StringSerializer extends BlockSerializer<String, org.apache.kafka.common.serialization.StringSerializer> {
    public StringSerializer() {
        super(new org.apache.kafka.common.serialization.StringSerializer());
    }
}
