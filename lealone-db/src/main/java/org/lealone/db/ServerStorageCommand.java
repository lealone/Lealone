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
package org.lealone.db;

import java.nio.ByteBuffer;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.Future;
import org.lealone.db.session.ServerSession;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageMap;
import org.lealone.storage.replication.ReplicaStorageCommand;
import org.lealone.transaction.Transaction;

public class ServerStorageCommand implements ReplicaStorageCommand {

    private final ServerSession session;

    public ServerStorageCommand(ServerSession session) {
        this.session = session;
    }

    @Override
    public int getType() {
        return SERVER_STORAGE_COMMAND;
    }

    @Override
    public Future<Object> put(String mapName, ByteBuffer key, ByteBuffer value, boolean raw) {
        return executeReplicaPut(null, mapName, key, value, raw);
    }

    @Override
    public Future<Object> executeReplicaPut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value,
            boolean raw) {
        session.setReplicationName(replicationName);
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        if (raw) {
            map = map.getRawMap();
        }
        Object result = map.put(map.getKeyType().read(key), map.getValueType().read(value));

        if (result == null)
            return null;

        try (DataBuffer b = DataBuffer.create()) {
            ByteBuffer valueBuffer = b.write(map.getValueType(), result);
            return Future.succeededFuture(valueBuffer.array());
        }
    }

    @Override
    public Future<Object> get(String mapName, ByteBuffer key) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        Object result = map.get(map.getKeyType().read(key));
        return Future.succeededFuture(result);
    }

    @Override
    public Future<Object> append(String mapName, ByteBuffer value) {
        return executeReplicaAppend(null, mapName, value);
    }

    @Override
    public Future<Object> executeReplicaAppend(String replicationName, String mapName, ByteBuffer value) {
        session.setReplicationName(replicationName);
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        Object result = map.append(map.getValueType().read(value));
        Transaction parentTransaction = session.getParentTransaction();
        if (parentTransaction != null && !parentTransaction.isAutoCommit()) {
            parentTransaction.addLocalTransactionNames(session.getTransaction().getLocalTransactionNames());
        }
        return Future.succeededFuture(result);
    }

    @Override
    public Future<LeafPageMovePlan> prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        return Future.succeededFuture(map.prepareMoveLeafPage(leafPageMovePlan));
    }

    @Override
    public void moveLeafPage(String mapName, PageKey pageKey, ByteBuffer page, boolean addPage) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        map.addLeafPage(pageKey, page, addPage);
    }

    @Override
    public void replicateRootPages(String dbName, ByteBuffer rootPages) {
        session.replicateRootPages(dbName, rootPages);
    }

    @Override
    public void removeLeafPage(String mapName, PageKey pageKey) {
        // 当前节点删除自己的 leaf page时不应该再触发自己再按 page key删一次
        throw DbException.throwInternalError();
    }

    @Override
    public void replicaCommit(long validKey, boolean autoCommit) {
        session.replicationCommit(validKey, autoCommit);
    }

    @Override
    public void replicaRollback() {
        session.rollback();
    }
}
