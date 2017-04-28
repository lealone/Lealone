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
import java.util.ArrayList;

import org.lealone.db.result.Result;
import org.lealone.db.value.ValueLong;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.WriteBuffer;
import org.lealone.transaction.Transaction;

public class ServerCommand extends CommandBase implements StorageCommand {

    private final ServerSession session;

    public ServerCommand(ServerSession session) {
        this.session = session;
    }

    @Override
    public int getType() {
        return SERVER_COMMAND;
    }

    @Override
    public boolean isQuery() {
        return false;
    }

    @Override
    public ArrayList<? extends CommandParameter> getParameters() {
        return null;
    }

    @Override
    public Result executeQuery(int maxRows) {
        return executeQuery(maxRows, false);
    }

    @Override
    public Result executeQuery(int maxRows, boolean scrollable) {
        return null;
    }

    @Override
    public int executeUpdate() {
        return 0;
    }

    @Override
    public int executeUpdate(String replicationName, CommandUpdateResult commandUpdateResult) {
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() {
    }

    @Override
    public Result getMetaData() {
        return null;
    }

    @Override
    public LeafPageMovePlan prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        return map.prepareMoveLeafPage(leafPageMovePlan);
    }

    @Override
    public void moveLeafPage(String mapName, ByteBuffer splitKey, ByteBuffer page) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        map.addLeafPage(splitKey, page);
    }

    @Override
    public void removeLeafPage(String mapName, ByteBuffer key) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        map.removeLeafPage(key);
    }

    @Override
    public Object executePut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value) {
        session.setReplicationName(replicationName);
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        Object result = map.put(map.getKeyType().read(key), map.getValueType().read(value));

        if (result == null)
            return null;

        try (WriteBuffer b = WriteBuffer.create()) {
            ByteBuffer valueBuffer = b.write(map.getValueType(), result);
            return valueBuffer.array();
        }
    }

    @Override
    public Object executeGet(String mapName, ByteBuffer key) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        Object result = map.get(map.getKeyType().read(key));
        return result;
    }

    @Override
    public Object executeAppend(String replicationName, String mapName, ByteBuffer value,
            CommandUpdateResult commandUpdateResult) {
        session.setReplicationName(replicationName);
        StorageMap<Object, Object> map = session.getStorageMap(mapName);
        Object result = map.append(map.getValueType().read(value));
        commandUpdateResult.addResult(this, ((ValueLong) result).getLong());
        Transaction parentTransaction = session.getParentTransaction();
        if (parentTransaction != null && !parentTransaction.isAutoCommit()) {
            parentTransaction.addLocalTransactionNames(session.getTransaction().getLocalTransactionNames());
        }
        return result;
    }

    @Override
    public void replicationCommit(long validKey, boolean autoCommit) {
        session.replicationCommit(validKey, autoCommit);
    }

    @Override
    public void replicationRollback() {
        session.rollback();
    }
}
