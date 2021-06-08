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

public abstract class StorageOperation implements Packet {

    public final String mapName;
    public final boolean isDistributedTransaction;

    public StorageOperation(String mapName, boolean isDistributedTransaction) {
        this.mapName = mapName;
        this.isDistributedTransaction = isDistributedTransaction;
    }

    public StorageOperation(NetInputStream in, int version) throws IOException {
        mapName = in.readString();
        isDistributedTransaction = in.readBoolean();
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(mapName);
        out.writeBoolean(isDistributedTransaction);
    }
}
