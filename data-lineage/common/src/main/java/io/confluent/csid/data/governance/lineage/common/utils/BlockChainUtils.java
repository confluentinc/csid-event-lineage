package io.confluent.csid.data.governance.lineage.common.utils;

import io.confluent.csid.data.governance.lineage.common.Block;

public class BlockChainUtils {

    public interface BlockSupplier {
        Block get(final String data);
    }

    public static boolean isValid(final Block block, final BlockSupplier blockSupplier) {
        if (!block.isValid()) {
            return false;
        }

        if (!block.isGenesisBlock()) {
            final Block previousBlock = blockSupplier.get(block.getPreviousHash());
            return (previousBlock != null) && isValid(previousBlock, blockSupplier);
        }

        return true;
    }

}
