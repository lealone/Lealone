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
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.storage.replication.ReplicationConflictType;

public class ReplicationPreparedUpdateAck extends ReplicationUpdateAck {

    public ReplicationPreparedUpdateAck(int updateCount, long key, long first, List<String> uncommittedReplicationNames,
            ReplicationConflictType replicationConflictType) {
        super(updateCount, key, first, uncommittedReplicationNames, replicationConflictType);
    }

    @Override
    public PacketType getType() {
        return PacketType.REPLICATION_PREPARED_UPDATE_ACK;
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReplicationPreparedUpdateAck> {
        @Override
        public ReplicationPreparedUpdateAck decode(NetInputStream in, int version) throws IOException {
            return new ReplicationPreparedUpdateAck(in.readInt(), in.readLong(), in.readLong(),
                    readUncommittedReplicationNames(in), ReplicationConflictType.getType(in.readInt()));
        }
    }
}
