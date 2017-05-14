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
package org.lealone.storage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.Session;
import org.lealone.db.value.ValueLong;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;

public abstract class StorageMapBase<K, V> implements StorageMap<K, V> {

    protected final String name;
    protected final DataType keyType;
    protected final DataType valueType;
    // TODO 考虑是否要使用总是递增的数字
    protected final AtomicLong lastKey = new AtomicLong(0);

    protected StorageMapBase(String name, DataType keyType, DataType valueType) {
        DataUtils.checkArgument(name != null, "The name may not be null");
        if (keyType == null) {
            keyType = new ObjectDataType();
        }
        if (valueType == null) {
            valueType = new ObjectDataType();
        }
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public DataType getKeyType() {
        return keyType;
    }

    @Override
    public DataType getValueType() {
        return valueType;
    }

    // 如果新key比lastKey大就更新lastKey
    // 允许多线程并发更新
    public void setLastKey(Object key) {
        if (key instanceof ValueLong) {
            long k = ((ValueLong) key).getLong();
            while (true) {
                long old = lastKey.get();
                if (k > old) {
                    if (lastKey.compareAndSet(old, k))
                        break;
                } else {
                    break;
                }
            }
        }
    }

    public long getLastKey() {
        return lastKey.get();
    }

    @Override
    public void addLeafPage(ByteBuffer splitKey, ByteBuffer page) {
        throw DbException.getUnsupportedException("addLeafPage");
    }

    @Override
    public void removeLeafPage(ByteBuffer key) {
        throw DbException.getUnsupportedException("removeLeafPage");
    }

    @Override
    public LeafPageMovePlan prepareMoveLeafPage(LeafPageMovePlan leafPageMovePlan) {
        throw DbException.getUnsupportedException("prepareMoveLeafPage");
    }

    @Override
    public List<NetEndpoint> getReplicationEndpoints(Object key) {
        throw DbException.getUnsupportedException("getReplicationEndpoints");
    }

    @Override
    public Object replicationPut(Session session, Object key, Object value, DataType valueType) {
        throw DbException.getUnsupportedException("put");
    }

    @Override
    public Object replicationGet(Session session, Object key) {
        throw DbException.getUnsupportedException("get");
    }

    @Override
    public Object replicationAppend(Session session, Object value, DataType valueType) {
        throw DbException.getUnsupportedException("append");
    }

    @SuppressWarnings("unchecked")
    @Override
    public StorageMap<Object, Object> getRawMap() {
        return (StorageMap<Object, Object>) this;
    }

}
