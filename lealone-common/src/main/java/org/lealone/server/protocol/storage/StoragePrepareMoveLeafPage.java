/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.storage;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.storage.LeafPageMovePlan;

public class StoragePrepareMoveLeafPage implements Packet {

    public final String mapName;
    public final LeafPageMovePlan leafPageMovePlan;

    public StoragePrepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan) {
        this.mapName = mapName;
        this.leafPageMovePlan = leafPageMovePlan;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_PREPARE_MOVE_LEAF_PAGE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STORAGE_PREPARE_MOVE_LEAF_PAGE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(mapName);
        leafPageMovePlan.serialize(out);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StoragePrepareMoveLeafPage> {
        @Override
        public StoragePrepareMoveLeafPage decode(NetInputStream in, int version) throws IOException {
            String mapName = in.readString();
            LeafPageMovePlan leafPageMovePlan = LeafPageMovePlan.deserialize(in);
            return new StoragePrepareMoveLeafPage(mapName, leafPageMovePlan);
        }
    }
}
