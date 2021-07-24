package io.confluent.data.csid.data.governance.lineage.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ProducerApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerApp.class);

    private static final ProducerService service = new ProducerService();

    public static void main(String[] args) {
        LOGGER.info("Starting streaming app.");

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streaming-app-shutdown-hook") {
            @Override
            public void run() {
                service.stop();
                latch.countDown();
            }
        });

        try {
            LOGGER.info("Streaming app started successfully.");

            service.start();

            latch.await();
        } catch (Throwable e) {
            service.stop();
            LOGGER.error("Streaming app ended with error.", e);
            System.exit(1);
        }

        LOGGER.info("Streaming app ended.");
        System.exit(0);
    }
}
