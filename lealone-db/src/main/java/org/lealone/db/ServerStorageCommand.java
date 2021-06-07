/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.nio.ByteBuffer;
import java.util.List;

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
    public Future<Object> get(String mapName, ByteBuffer key) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        Object result = map.get(map.getKeyType().read(key));
        return Future.succeededFuture(result);
    }

    @Override
    public Future<Object> put(String mapName, ByteBuffer key, ByteBuffer value, boolean raw, boolean addIfAbsent) {
        return executeReplicaPut(null, mapName, key, value, raw, addIfAbsent);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Object> executeReplicaPut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value,
            boolean raw, boolean addIfAbsent) {
        session.setReplicationName(replicationName);
        TransactionMap<Object, Object> tmap = session.getTransactionMap(mapName);
        AsyncCallback<Object> ac = new AsyncCallback<>();
        if (addIfAbsent) {
            tmap.addIfAbsent(tmap.getKeyType().read(key), tmap.getValueType().read(value)).onSuccess(r -> {
                ByteBuffer resultByteBuffer = ByteBuffer.allocate(1);
                resultByteBuffer.put((byte) 1);
                resultByteBuffer.flip();
                ac.setAsyncResult(resultByteBuffer);
            }).onFailure(t -> {
                ByteBuffer resultByteBuffer = ByteBuffer.allocate(1);
                resultByteBuffer.put((byte) 0);
                resultByteBuffer.flip();
                ac.setAsyncResult(resultByteBuffer);
            });
            ;
        } else {
            StorageMap<Object, Object> map = tmap;
            if (raw) {
                map = (StorageMap<Object, Object>) tmap.getRawMap();
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
    public Future<Object> append(String mapName, ByteBuffer value) {
        return executeReplicaAppend(null, mapName, value);
    }

    @Override
    public Future<Object> executeReplicaAppend(String replicationName, String mapName, ByteBuffer value) {
        session.setReplicationName(replicationName);
        TransactionMap<Object, Object> map = session.getTransactionMap(mapName);
        Object result = map.append(map.getValueType().read(value));
        Transaction parentTransaction = session.getParentTransaction();
        if (parentTransaction != null && !parentTransaction.isAutoCommit()) {
            parentTransaction.addLocalTransactionNames(session.getTransaction().getLocalTransactionNames());
        }
        return Future.succeededFuture(result);
    }

    @Override
    public Future<Boolean> replace(String mapName, ByteBuffer key, ByteBuffer oldValue, ByteBuffer newValue) {
        return executeReplicaReplace(null, mapName, key, oldValue, newValue);
    }

    @Override
    public Future<Boolean> executeReplicaReplace(String replicationName, String mapName, ByteBuffer key,
            ByteBuffer oldValue, ByteBuffer newValue) {
        session.setReplicationName(replicationName);
        TransactionMap<Object, Object> map = session.getTransactionMap(mapName);
        Boolean result = map.replace(map.getKeyType().read(key), map.getValueType().read(oldValue),
                map.getValueType().read(newValue));
        Transaction parentTransaction = session.getParentTransaction();
        if (parentTransaction != null && !parentTransaction.isAutoCommit()) {
            parentTransaction.addLocalTransactionNames(session.getTransaction().getLocalTransactionNames());
        }
        return Future.succeededFuture(result);
    }

    @Override
    public Future<Object> remove(String mapName, ByteBuffer key) {
        return executeReplicaRemove(null, mapName, key);
    }

    @Override
    public Future<Object> executeReplicaRemove(String replicationName, String mapName, ByteBuffer key) {
        session.setReplicationName(replicationName);
        TransactionMap<Object, Object> map = session.getTransactionMap(mapName);
        Object result = map.remove(map.getKeyType().read(key));
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
        throw DbException.getInternalError();
    }

    @Override
    public void handleReplicaConflict(List<String> retryReplicationNames) {
        session.handleReplicaConflict(retryReplicationNames);
    }
}
