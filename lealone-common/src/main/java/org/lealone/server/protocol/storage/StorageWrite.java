/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.storage;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;

public abstract class StorageWrite extends StorageOperation {

    public final String replicationName;

    public StorageWrite(String mapName, boolean isDistributedTransaction, String replicationName) {
        super(mapName, isDistributedTransaction);
        this.replicationName = replicationName;
    }

    public StorageWrite(NetInputStream in, int version) throws IOException {
        super(in, version);
        replicationName = in.readString();
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeString(replicationName);
    }
}
