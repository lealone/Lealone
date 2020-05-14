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
package org.lealone.db.session;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.trace.TraceObject;
import org.lealone.common.trace.TraceObjectType;
import org.lealone.common.trace.TraceSystem;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.DbSetting;
import org.lealone.db.RunMode;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.storage.fs.FileUtils;
import org.lealone.transaction.Transaction;

public abstract class SessionBase implements Session {

    protected String replicationName;
    protected AtomicInteger nextId = new AtomicInteger(0);

    protected String targetNodes;
    protected RunMode runMode;
    protected boolean invalid;

    protected boolean autoCommit = true;
    protected Transaction parentTransaction;

    protected String newTargetNodes;

    protected TraceSystem traceSystem;
    protected boolean closed;

    @Override
    public String getReplicationName() {
        return replicationName;
    }

    @Override
    public void setReplicationName(String replicationName) {
        this.replicationName = replicationName;
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public boolean isShardingMode() {
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
    public void asyncCommit(Runnable asyncTask) {
    }

    @Override
    public void asyncCommitComplete() {
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
    public boolean isValid() {
        return !invalid;
    }

    @Override
    public void setTargetNodes(String targetNodes) {
        this.targetNodes = targetNodes;
    }

    @Override
    public String getTargetNodes() {
        return targetNodes;
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
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public Transaction getTransaction() {
        throw DbException.getUnsupportedException("getTransaction");
    }

    @Override
    public Transaction getTransaction(PreparedSQLStatement statement) {
        throw DbException.getUnsupportedException("getTransaction");
    }

    @Override
    public Transaction getParentTransaction() {
        return parentTransaction;
    }

    // 要加synchronized，避免ClientCommand在执行更新和查询时其他线程把transaction置null
    @Override
    public synchronized void setParentTransaction(Transaction parentTransaction) {
        this.parentTransaction = parentTransaction;
    }

    @Override
    public boolean isRunModeChanged() {
        return newTargetNodes != null;
    }

    @Override
    public String getNewTargetNodes() {
        String nodes = newTargetNodes;
        newTargetNodes = null;
        return nodes;
    }

    @Override
    public void runModeChanged(String newTargetNodes) {
        this.newTargetNodes = newTargetNodes;
    }

    public Trace getTrace(TraceModuleType traceModuleType) {
        if (traceSystem != null)
            return traceSystem.getTrace(traceModuleType);
        else
            return Trace.NO_TRACE;
    }

    @Override
    public Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType) {
        if (traceSystem != null)
            return traceSystem.getTrace(traceModuleType, traceObjectType, TraceObject.getNextTraceId(traceObjectType));
        else
            return Trace.NO_TRACE;
    }

    @Override
    public Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType, int traceObjectId) {
        if (traceSystem != null)
            return traceSystem.getTrace(traceModuleType, traceObjectType, traceObjectId);
        else
            return Trace.NO_TRACE;
    }

    protected void initTraceSystem(ConnectionInfo ci) {
        if (traceSystem != null || ci.isTraceDisabled())
            return;
        traceSystem = new TraceSystem();
        String traceLevelFile = ci.getProperty(DbSetting.TRACE_LEVEL_FILE.getName(), null);
        if (traceLevelFile != null) {
            int level = Integer.parseInt(traceLevelFile);
            String prefix = getFilePrefix(SysProperties.CLIENT_TRACE_DIRECTORY, ci.getDatabaseName());
            try {
                traceSystem.setLevelFile(level);
                if (level > 0) {
                    String file = FileUtils.createTempFile(prefix, Constants.SUFFIX_TRACE_FILE, false, false);
                    traceSystem.setFileName(file);
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, prefix);
            }
        }
        String traceLevelSystemOut = ci.getProperty(DbSetting.TRACE_LEVEL_SYSTEM_OUT.getName(), null);
        if (traceLevelSystemOut != null) {
            int level = Integer.parseInt(traceLevelSystemOut);
            traceSystem.setLevelSystemOut(level);
        }
    }

    protected void closeTraceSystem() {
        if (traceSystem != null) {
            traceSystem.close();
            traceSystem = null;
        }
    }

    private static String getFilePrefix(String dir, String dbName) {
        StringBuilder buff = new StringBuilder(dir);
        if (!(dir.charAt(dir.length() - 1) == File.separatorChar))
            buff.append(File.separatorChar);
        for (int i = 0, length = dbName.length(); i < length; i++) {
            char ch = dbName.charAt(i);
            if (Character.isLetterOrDigit(ch)) {
                buff.append(ch);
            } else {
                buff.append('_');
            }
        }
        return buff.toString();
    }

    /**
     * Check if this session is closed and throws an exception if so.
     *
     * @throws DbException if the session is closed
     */
    @Override
    public void checkClosed() {
        if (isClosed()) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "session closed");
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }
}
