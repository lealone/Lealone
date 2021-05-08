/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    public final long key;
    public final long first;
    public String uncommittedReplicationName;
    public final ReplicationConflictType replicationConflictType;
    public final int ackVersion; // 复制操作有可能返回多次，这个字段表示第几次返回响应结果
    public final boolean isIfDDL;
    private ReplicaCommand replicaCommand;
    private int packetId;

    public ReplicationUpdateAck(int updateCount, long key, long first, String uncommittedReplicationName,
            ReplicationConflictType replicationConflictType, int ackVersion, boolean isIfDDL) {
        super(updateCount);
        this.key = key;
        this.first = first;
        this.uncommittedReplicationName = uncommittedReplicationName;
        this.replicationConflictType = replicationConflictType == null ? ReplicationConflictType.NONE
                : replicationConflictType;
        this.ackVersion = ackVersion;
        this.isIfDDL = isIfDDL;
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
        out.writeLong(key);
        out.writeLong(first);
        out.writeString(uncommittedReplicationName);
        out.writeInt(replicationConflictType.value);
        out.writeInt(ackVersion);
        out.writeBoolean(isIfDDL);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReplicationUpdateAck> {
        @Override
        public ReplicationUpdateAck decode(NetInputStream in, int version) throws IOException {
            return new ReplicationUpdateAck(in.readInt(), in.readLong(), in.readLong(), in.readString(),
                    ReplicationConflictType.getType(in.readInt()), in.readInt(), in.readBoolean());
        }
    }
}
