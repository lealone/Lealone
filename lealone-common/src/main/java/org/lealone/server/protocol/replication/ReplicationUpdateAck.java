/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.replication;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.statement.StatementUpdateAck;
import org.lealone.storage.replication.ReplicaCommand;
import org.lealone.storage.replication.ReplicationConflictType;

public class ReplicationUpdateAck extends StatementUpdateAck {

    public final long first;
    public String uncommittedReplicationName;
    public final ReplicationConflictType replicationConflictType;
    public final int ackVersion; // 复制操作有可能返回多次，这个字段表示第几次返回响应结果
    public final boolean isIfDDL;
    public final boolean isFinalResult;
    private ReplicaCommand replicaCommand;
    private int packetId;

    public ReplicationUpdateAck(int updateCount, long first, String uncommittedReplicationName,
            ReplicationConflictType replicationConflictType, int ackVersion, boolean isIfDDL, boolean isFinalResult) {
        super(updateCount);
        this.first = first;
        this.uncommittedReplicationName = uncommittedReplicationName;
        this.replicationConflictType = replicationConflictType == null ? ReplicationConflictType.NONE
                : replicationConflictType;
        this.ackVersion = ackVersion;
        this.isIfDDL = isIfDDL;
        this.isFinalResult = isFinalResult;
    }

    public ReplicationUpdateAck(NetInputStream in, int version) throws IOException {
        super(in, version);
        first = in.readLong();
        uncommittedReplicationName = in.readString();
        replicationConflictType = ReplicationConflictType.getType(in.readInt());
        ackVersion = in.readInt();
        isIfDDL = in.readBoolean();
        isFinalResult = in.readBoolean();
    }

    @Override
    public PacketType getType() {
        return PacketType.REPLICATION_UPDATE_ACK;
    }

    public ReplicaCommand getReplicaCommand() {
        return replicaCommand;
    }

    public void setReplicaCommand(ReplicaCommand replicaCommand) {
        this.replicaCommand = replicaCommand;
    }

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeLong(first);
        out.writeString(uncommittedReplicationName);
        out.writeInt(replicationConflictType.value);
        out.writeInt(ackVersion);
        out.writeBoolean(isIfDDL);
        out.writeBoolean(isFinalResult);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReplicationUpdateAck> {
        @Override
        public ReplicationUpdateAck decode(NetInputStream in, int version) throws IOException {
            return new ReplicationUpdateAck(in, version);
        }
    }
}
