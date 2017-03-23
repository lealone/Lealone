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

import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.storage.StorageMap;

public abstract class SessionBase implements Session {

    protected String replicationName;
    protected boolean local;
    protected AtomicInteger nextId = new AtomicInteger(0);
    protected Runnable runnable;

    protected String targetEndpoints;
    protected RunMode runMode;
    protected boolean invalid;

    protected boolean autoCommit = true;

    @Override
    public String getReplicationName() {
        return replicationName;
    }

    @Override
    public void setReplicationName(String replicationName) {
        this.replicationName = replicationName;
    }

    @Override
    public void setLocal(boolean local) {
        this.local = local;
    }

    @Override
    public boolean isLocal() {
        return local;
    }

    @Override
    public boolean isShardingMode() {
        return false;
    }

    @Override
    public StorageMap<Object, Object> getStorageMap(String mapName) {
        throw DbException.getUnsupportedException("getStorageMap");
    }

    @Override
    public boolean containsTransaction() {
        return false;
    }

    @Override
    public int getNextId() {
        return nextId.incrementAndGet();
    }

    public int getCurrentId() {
        return nextId.get();
    }

    @Override
    public void setRunnable(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public Runnable getRunnable() {
        return runnable;
    }

    @Override
    public void prepareCommit() {
    }

    @Override
    public SessionStatus getStatus() {
        return SessionStatus.NO_TRANSACTION;
    }

    @Override
    public void setInvalid(boolean v) {
        invalid = v;
    }

    @Override
    public boolean isInvalid() {
        return invalid;
    }

    @Override
    public void setTargetEndpoints(String targetEndpoints) {
        this.targetEndpoints = targetEndpoints;
    }

    @Override
    public String getTargetEndpoints() {
        return targetEndpoints;
    }

    @Override
    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    @Override
    public RunMode getRunMode() {
        return runMode;
    }

    @Override
    public long getLastRowKey() {
        return 0;
    }

    @Override
    public void replicationCommit(long validKey, boolean autoCommit) {
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

}
