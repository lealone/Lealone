/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.storage;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.storage.page.LeafPageMovePlan;

public class StoragePrepareMoveLeafPageAck implements AckPacket {

    public final LeafPageMovePlan leafPageMovePlan;

    public StoragePrepareMoveLeafPageAck(LeafPageMovePlan leafPageMovePlan) {
        this.leafPageMovePlan = leafPageMovePlan;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_PREPARE_MOVE_LEAF_PAGE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        leafPageMovePlan.serialize(out);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StoragePrepareMoveLeafPageAck> {
        @Override
        public StoragePrepareMoveLeafPageAck decode(NetInputStream in, int version) throws IOException {
            LeafPageMovePlan leafPageMovePlan = LeafPageMovePlan.deserialize(in);
            return new StoragePrepareMoveLeafPageAck(leafPageMovePlan);
        }
    }
}
