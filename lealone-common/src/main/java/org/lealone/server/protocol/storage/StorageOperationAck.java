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

public abstract class StorageOperationAck implements AckPacket {

    public StorageOperationAck() {
    }

    public StorageOperationAck(NetInputStream in, int version) throws IOException {
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
    }
}
