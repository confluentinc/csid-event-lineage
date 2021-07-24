package io.confluent.data.csid.data.governance.lineage.consumer;

import io.confluent.data.lineage.utils.BlockUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsumerApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerApp.class);

    private final static Map<Integer, Integer> partitionOffsets = new HashMap<>();

    public static void main(final String[] args) throws IOException, InterruptedException {

        // Get the config file name from args
        LOGGER.info("Loading client properties.");
        Properties clientProperties = BlockUtils.loadProperties("consumer.properties", ConsumerApp.class);

        //
        final String topicName = clientProperties.getProperty("topic.name");

        LOGGER.info("Receiving events from topic '{}'.", topicName);

        // we will then create a producer based off a class that was generated
        // from a schema in the schema registry
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientProperties)) {

            // Subscribe to our topic
            consumer.subscribe(Collections.singletonList(topicName));

            // some data to be used at random
            while (true) {
                try {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("Received: {}", record.value());
                    }
                    consumer.commitSync();
                } catch (SerializationException e) {
                    Pattern pattern = Pattern.compile("Error deserializing key\\/value for partition aTopic-([0-9])* at offset ([0-9])*. If needed, please seek past the record to continue consumption.");
                    Matcher matcher = pattern.matcher(e.getMessage());
                    if (matcher.find()) {
                        final int partition = Integer.parseInt(matcher.group(1));
                        final int offset = Integer.parseInt(matcher.group(2));
                        final int nextOffset = offset + 1;

                        int lastOffset = partitionOffsets.computeIfAbsent(partition, (p) -> offset);
                        if (lastOffset > offset) {
                            consumer.commitSync(new HashMap<>() {{
                                put(new TopicPartition(topicName, partition) , new OffsetAndMetadata(lastOffset+1));
                            }});
                            LOGGER.info("Commit partition '{}' to offset '{}:{}'.", partition, offset, lastOffset);
                            partitionOffsets.put(partition, -1);
                        } else if (lastOffset != -1) {
                            consumer.seek(new TopicPartition(topicName, partition), nextOffset);
                            LOGGER.info("Seek partition '{}' to offset '{}:{}'.", partition, offset, nextOffset);

                            partitionOffsets.put(partition, offset);
                        }
                    }
                }
            }

        } catch (final Throwable e) {
            e.printStackTrace();
        }
    }
}
