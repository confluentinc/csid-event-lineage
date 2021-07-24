package io.confluent.data.csid.data.governance.lineage.miner.dao;

import io.confluent.data.lineage.Block;

public interface BlockDAO {

    boolean isOpen();
    void put(Block block);
    Block get(final String hash);

}
