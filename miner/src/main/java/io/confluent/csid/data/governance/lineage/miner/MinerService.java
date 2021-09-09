package io.confluent.csid.data.governance.lineage.miner;

import io.confluent.csid.data.governance.lineage.common.Block;
import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MinerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MinerService.class);
    public static String STORE_NAME = "Blocks";

    private KafkaStreams kafkaStreams;

    public MinerService() {
    }

    public void start() {
        LOGGER.info("Starting Stream service....");

        final Map<String, ?> properties = BlockUtils.loadConfigurationFromResource("minerapp.properties", MinerService.class);

        final KafkaJsonSchemaSerde<Block> serde = new KafkaJsonSchemaSerde<>();
        serde.configure(properties, false);

        final String inTopic = (String) properties.get("input.topic");
        final String outTopic = (String) properties.get("output.topic");

        // Topology
        final Topology builder = new Topology();

        Materialized<String, Block, KeyValueStore<Bytes, byte[]>> materializedStore =
                Materialized.<String, Block>as(Stores.persistentKeyValueStore(STORE_NAME))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(serde)
                        .withCachingEnabled();

        final KeyValueStoreBuilder<String, Block> storeBuilder = new KeyValueStoreBuilder<>(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                serde,
                Time.SYSTEM);

        builder.addSource("source", new StringDeserializer(), serde.deserializer(), inTopic)
                .addProcessor("mine", new ProcSupplier(), "source")
                .addSink("destination", outTopic, new StringSerializer(), serde.serializer(), "mine")
                .addStateStore(Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(STORE_NAME),
                        Serdes.String(),
                        serde), "mine");

        LOGGER.info(builder.describe().toString());

        kafkaStreams = new KafkaStreams(builder, BlockUtils.loadProperties("minerapp.properties", MinerService.class));
        kafkaStreams.start();

        LOGGER.info("Stream service started....");
    }

    public void stop() {
        LOGGER.info("Stopping Stream Service....");
        kafkaStreams.close();
        LOGGER.info("Stream service stopped....");
    }

    public static class ProcSupplier implements ProcessorSupplier<String, Block> {

        @Override
        public Processor<String, Block> get() {
            return new Miner();
        }
    }
}
