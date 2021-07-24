package io.confluent.csid.data.governance.lineage.common;

import org.apache.kafka.common.Configurable;

public interface BlockProducer extends Configurable {

    void sendBlock(final byte[] data);
    void close();

}
