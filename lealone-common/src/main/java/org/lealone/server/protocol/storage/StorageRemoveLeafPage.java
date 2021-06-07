/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.storage;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.NoAckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.storage.PageKey;

public class StorageRemoveLeafPage implements NoAckPacket {

    public final String mapName;
    public final PageKey pageKey;

    public StorageRemoveLeafPage(String mapName, PageKey pageKey) {
        this.mapName = mapName;
        this.pageKey = pageKey;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_REMOVE_LEAF_PAGE;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(mapName);
        out.writePageKey(pageKey);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageRemoveLeafPage> {
        @Override
        public StorageRemoveLeafPage decode(NetInputStream in, int version) throws IOException {
            String mapName = in.readString();
            PageKey pageKey = in.readPageKey();
            return new StorageRemoveLeafPage(mapName, pageKey);
        }
    }
}
