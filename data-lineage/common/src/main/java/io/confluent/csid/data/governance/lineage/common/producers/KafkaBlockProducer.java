package io.confluent.csid.data.governance.lineage.common.producers;

import io.confluent.csid.data.governance.lineage.common.Block;
import io.confluent.csid.data.governance.lineage.common.BlockProducer;
import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.security.NoSuchAlgorithmException;
import java.util.Map;

@Slf4j()
public class KafkaBlockProducer implements BlockProducer {
    private final static String BLOCK_PRODUCER_PREFIX = "block.producer";
    private final static String MINER_MINE = BLOCK_PRODUCER_PREFIX + ".mine";
    private final static String BLOCK_TOPIC = BLOCK_PRODUCER_PREFIX + ".topic";

    private KafkaProducer<String, Block> producer;
    private String blockTopic;
    private boolean shouldMine;

    @Override
    public void sendBlock(final byte[] data) {
        try {
            final Block block = new Block(data);
            if (shouldMine) {
                block.mineBlock(BlockUtils.prefix);
            }

            producer.send(new ProducerRecord<>(blockTopic, block.getData(), block));
            producer.flush();
        } catch (NoSuchAlgorithmException e) {
            log.error("Error creating block.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            producer = null;
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        blockTopic = configs.get(BLOCK_TOPIC).toString();

        final String shouldMineStr = (String)configs.get(MINER_MINE);
        shouldMine = (shouldMineStr != null && !shouldMineStr.isEmpty() && Boolean.parseBoolean(shouldMineStr));

        Map<String, Object> blockProperties = BlockUtils.filterConfiguration(configs, BLOCK_PRODUCER_PREFIX);
        blockProperties.put("key.serializer", StringSerializer.class.getCanonicalName());
        blockProperties.put("value.serializer", KafkaJsonSchemaSerializer.class.getCanonicalName());

        producer = new KafkaProducer<>(blockProperties);
    }
}
