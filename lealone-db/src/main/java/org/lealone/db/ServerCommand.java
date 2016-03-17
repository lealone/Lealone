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
import org.lealone.replication.Replication;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.DataType;

public class ServerCommand implements StorageCommand {

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
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ArrayList<? extends CommandParameter> getParameters() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Result query(int maxRows) {
        return query(maxRows, false);
    }

    @Override
    public Result query(int maxRows, boolean scrollable) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int update(String replicationName) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void cancel() {
        // TODO Auto-generated method stub

    }

    @Override
    public Result getMetaData() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object executePut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value) {
        session.setReplicationName(replicationName);

        StorageMap<Object, Object> map = session.getStorageMap(mapName);

        DataType valueType = map.getValueType();
        // synchronized (session) {
        Object result = map.put(map.getKeyType().read(key), valueType.read(value));
        // }

        return result;
    }

    @Override
    public Object executeGet(String mapName, ByteBuffer key) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);

        // synchronized (session) {
        Object result = map.get(map.getKeyType().read(key));
        // }

        return result;
    }

    @Override
    public void moveLeafPage(String mapName, ByteBuffer splitKey, ByteBuffer page) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);

        if (map instanceof Replication) {
            ((Replication) map).addLeafPage(splitKey, page);
        }
    }

    @Override
    public void removeLeafPage(String mapName, ByteBuffer key) {
        StorageMap<Object, Object> map = session.getStorageMap(mapName);

        if (map instanceof Replication) {
            ((Replication) map).removeLeafPage(key);
        }
    }

    @Override
    public Command prepare() {
        return this;
    }

}
