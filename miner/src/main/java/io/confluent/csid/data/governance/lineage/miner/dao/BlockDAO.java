package io.confluent.csid.data.governance.lineage.miner.dao;


import io.confluent.csid.data.governance.lineage.common.Block;

public interface BlockDAO {

    boolean isOpen();

    void put(Block block);

    Block get(final String hash);

    default void close() {
        //noop default
    }
}
