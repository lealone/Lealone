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
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;
import org.lealone.db.session.ServerSession;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageMap;
import org.lealone.storage.replication.ReplicaStorageCommand;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;

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
    public Future<Object> put(String mapName, ByteBuffer key, ByteBuffer value, boolean raw, boolean addIfAbsent) {
        return executeReplicaPut(null, mapName, key, value, raw, addIfAbsent);
    }

    @Override
    public Future<Object> executeReplicaPut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value,
            boolean raw, boolean addIfAbsent) {
        session.setReplicationName(replicationName);
        AsyncCallback<Object> ac = new AsyncCallback<>();
        if (addIfAbsent) {
            Transaction.Listener localListener = new Transaction.Listener() {
                @Override
                public void operationUndo() {
                    ByteBuffer resultByteBuffer = ByteBuffer.allocate(1);
                    resultByteBuffer.put((byte) 0);
                    ac.setAsyncResult(resultByteBuffer);
                }

                @Override
                public void operationComplete() {
                    ByteBuffer resultByteBuffer = ByteBuffer.allocate(1);
                    resultByteBuffer.put((byte) 1);
                    ac.setAsyncResult(resultByteBuffer);
                }
            };
            TransactionMap<Object, Object> map = session.getTransactionMap(mapName);
            map.addIfAbsent(map.getKeyType().read(key), map.getValueType().read(value), localListener);
        } else {
            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            if (raw) {
                map = map.getRawMap();
            }
            StorageDataType valueType = map.getValueType();
            map.put(map.getKeyType().read(key), valueType.read(value), ar -> {
                if (ar.isSucceeded()) {
                    Object result = ar.getResult();
                    if (result != null) {
                        try (DataBuffer b = DataBuffer.create()) {
                            ByteBuffer valueBuffer = b.write(valueType, result);
                            result = valueBuffer.array();
                        }
                    }
                    ac.setAsyncResult(result);
                } else {
                    ac.setAsyncResult(ar.getCause());
                }
            });
        }
        return ac;
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
    public void replicatePages(String dbName, String storageName, ByteBuffer pages) {
        session.replicatePages(dbName, storageName, pages);
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
