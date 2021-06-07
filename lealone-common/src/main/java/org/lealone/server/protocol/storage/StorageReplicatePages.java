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

public class StorageReplicatePages implements NoAckPacket {

    public final String dbName;
    public final String storageName;
    public final ByteBuffer pages;

    public StorageReplicatePages(String dbName, String storageName, ByteBuffer pages) {
        this.dbName = dbName;
        this.storageName = storageName;
        this.pages = pages;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_REPLICATE_PAGES;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(dbName);
        out.writeString(storageName);
        out.writeByteBuffer(pages);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageReplicatePages> {
        @Override
        public StorageReplicatePages decode(NetInputStream in, int version) throws IOException {
            String dbName = in.readString();
            String storageName = in.readString();
            ByteBuffer pages = in.readByteBuffer();
            return new StorageReplicatePages(dbName, storageName, pages);
        }
    }
}
