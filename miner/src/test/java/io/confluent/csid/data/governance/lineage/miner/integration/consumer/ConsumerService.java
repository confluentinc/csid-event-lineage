package io.confluent.csid.data.governance.lineage.miner.integration.consumer;

import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);

    private final Map<Integer, Integer> partitionOffsets = new HashMap<>();

    private final KafkaConsumer<String, String> consumer;

    private String topic;

    public ConsumerService(Properties brokerProps) {
        Properties clientProperties = BlockUtils.loadProperties("consumer.properties", ConsumerService.class);
        brokerProps.stringPropertyNames().forEach(key -> clientProperties.setProperty(key, brokerProps.getProperty(key)));
        this.consumer = new KafkaConsumer<>(clientProperties);
    }

    public void subscribe(String topic) {
        this.topic = topic;
        consumer.subscribe(Collections.singleton(topic));
        partitionOffsets.clear();
    }

    public void unsubscribe() {
        consumer.unsubscribe();
    }

    public void poll(Consumer<ConsumerRecord<String, String>> recordConsumer) {
        try {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Received: {}", record.value());
                recordConsumer.accept(record);
            }
            consumer.commitSync();
        } catch (SerializationException e) {
            Pattern pattern = Pattern.compile("Error deserializing key\\/value for partition aTopic-(\\d*) at offset (\\d*)."
                + " If needed, please seek past the record to continue consumption.");
            Matcher matcher = pattern.matcher(e.getMessage());
            if (matcher.find()) {
                final int partition = Integer.parseInt(matcher.group(1));
                final int offset = Integer.parseInt(matcher.group(2));
                final int nextOffset = offset + 1;

                int lastOffset = partitionOffsets.computeIfAbsent(partition, (p) -> offset);
                if (lastOffset > offset) {
                    consumer.commitSync(new HashMap<>() {{
                        put(new TopicPartition(topic, partition), new OffsetAndMetadata(lastOffset + 1));
                    }});
                    LOGGER.info("Commit partition '{}' to offset '{}:{}'.", partition, offset, lastOffset);
                    partitionOffsets.put(partition, -1);
                } else if (lastOffset != -1) {
                    consumer.seek(new TopicPartition(this.topic, partition), nextOffset);
                    LOGGER.info("Seek partition '{}' to offset '{}:{}'.", partition, offset, nextOffset);

                    partitionOffsets.put(partition, offset);
                }
            }
        }
    }

    public void close() {
        consumer.unsubscribe();
        consumer.close();
    }
}
