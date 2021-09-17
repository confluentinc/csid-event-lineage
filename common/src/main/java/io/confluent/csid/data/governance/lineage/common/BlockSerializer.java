package io.confluent.csid.data.governance.lineage.common;

import io.confluent.csid.data.governance.lineage.common.producers.RestBlockProducer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@RequiredArgsConstructor()
@Slf4j()
public class BlockSerializer<T, S extends Serializer<T>> implements Serializer<T> {
    @NonNull private final S innerSerializer;
    private final BlockProducer blockProducer = new RestBlockProducer(); //new KafkaBlockProducer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        innerSerializer.configure(configs, isKey);
        blockProducer.configure(configs);
    }

    @Override
    public byte[] serialize(String s, T t) {
        final byte[] serialized = innerSerializer.serialize(s, t);
        if (serialized != null) {
            blockProducer.sendBlock(serialized);
        }

        return serialized;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        final byte[] serialized = innerSerializer.serialize(topic, headers, data);
        if (serialized != null) {
            blockProducer.sendBlock(serialized);
        }

        return serialized;
    }

    @Override
    public void close() {
        innerSerializer.close();
        blockProducer.close();
    }
}
