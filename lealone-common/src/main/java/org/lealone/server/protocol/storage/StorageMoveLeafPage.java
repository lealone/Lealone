/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.NoAckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.storage.page.PageKey;

public class StorageMoveLeafPage implements NoAckPacket {

    public final String mapName;
    public final PageKey pageKey;
    public final ByteBuffer page;
    public final boolean addPage;

    public StorageMoveLeafPage(String mapName, PageKey pageKey, ByteBuffer page, boolean addPage) {
        this.mapName = mapName;
        this.pageKey = pageKey;
        this.page = page;
        this.addPage = addPage;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_MOVE_LEAF_PAGE;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(mapName);
        out.writePageKey(pageKey);
        out.writeByteBuffer(page);
        out.writeBoolean(addPage);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageMoveLeafPage> {
        @Override
        public StorageMoveLeafPage decode(NetInputStream in, int version) throws IOException {
            String mapName = in.readString();
            PageKey pageKey = in.readPageKey();
            ByteBuffer page = in.readByteBuffer();
            boolean addPage = in.readBoolean();
            return new StorageMoveLeafPage(mapName, pageKey, page, addPage);
        }
    }
}
