package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import java.util.concurrent.CountDownLatch;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class SmtTracingTest {

  @SneakyThrows
  @Test
  void testSMTCaptureSourceTask(){
    CommonTestUtils commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
    ConnectStandalone connectStandalone = new ConnectStandalone();
    CountDownLatch connectLatch = new CountDownLatch(1);
    new Thread(()-> {
      connectStandalone.start(commonTestUtils.getConnectWorkerProperties(null),
          commonTestUtils.getSourceTaskProperties(null, "test-topic"));
      try {

        connectLatch.await();
      }catch (InterruptedException e){}
      finally {
        connectStandalone.stop();
      }
    }).start();
    Thread.sleep(5000);
    connectLatch.countDown();
    commonTestUtils.stopKafkaContainer();
  }
}
