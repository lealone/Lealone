/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.NoAckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class ReplicationHandleReplicaConflict implements NoAckPacket {

    public final List<String> retryReplicationNames;

    public ReplicationHandleReplicaConflict(List<String> retryReplicationNames) {
        this.retryReplicationNames = retryReplicationNames;
    }

    @Override
    public PacketType getType() {
        return PacketType.REPLICATION_HANDLE_REPLICA_CONFLICT;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        if (retryReplicationNames == null || retryReplicationNames.isEmpty()) {
            out.writeInt(0);
        } else {
            out.writeInt(retryReplicationNames.size());
            for (String name : retryReplicationNames)
                out.writeString(name);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReplicationHandleReplicaConflict> {
        @Override
        public ReplicationHandleReplicaConflict decode(NetInputStream in, int version) throws IOException {
            return new ReplicationHandleReplicaConflict(readRetryReplicationNames(in));
        }
    }

    private static List<String> readRetryReplicationNames(NetInputStream in) throws IOException {
        int size = in.readInt();
        if (size == 0)
            return null;
        List<String> retryReplicationNames = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            retryReplicationNames.add(in.readString());
        }
        return retryReplicationNames;
    }
}
