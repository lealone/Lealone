/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import java.io.File;
import java.io.IOException;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.trace.Trace;
import com.lealone.common.trace.TraceModuleType;
import com.lealone.common.trace.TraceObject;
import com.lealone.common.trace.TraceObjectType;
import com.lealone.common.trace.TraceSystem;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.Constants;
import com.lealone.db.DbSetting;
import com.lealone.db.RunMode;
import com.lealone.db.SysProperties;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.storage.fs.FileUtils;

public abstract class SessionBase implements Session {

    protected boolean autoCommit = true;
    protected boolean closed;

    protected boolean invalid;
    protected String targetNodes;
    protected RunMode runMode;
    protected int consistencyLevel;

    protected TraceSystem traceSystem;

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
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

    public void setConsistencyLevel(int consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public int getConsistencyLevel() {
        return consistencyLevel;
    }

    @Override
    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    @Override
    public RunMode getRunMode() {
        return runMode;
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
            return traceSystem.getTrace(traceModuleType, traceObjectType,
                    TraceObject.getNextTraceId(traceObjectType));
        else
            return Trace.NO_TRACE;
    }

    @Override
    public Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType,
            int traceObjectId) {
        if (traceSystem != null)
            return traceSystem.getTrace(traceModuleType, traceObjectType, traceObjectId);
        else
            return Trace.NO_TRACE;
    }

    protected void initTraceSystem(ConnectionInfo ci) {
        String filePrefix = getFilePrefix(SysProperties.CLIENT_TRACE_DIRECTORY, ci.getDatabaseName());
        initTraceSystem(ci, filePrefix);
    }

    protected void initTraceSystem(ConnectionInfo ci, String filePrefix) {
        if (traceSystem != null || ci.isTraceDisabled())
            return;
        traceSystem = new TraceSystem();
        String traceLevelFile = ci.getProperty(DbSetting.TRACE_LEVEL_FILE.getName(), null);
        if (traceLevelFile != null) {
            int level = Integer.parseInt(traceLevelFile);
            try {
                traceSystem.setLevelFile(level);
                if (level > 0) {
                    String file = FileUtils.createTempFile(filePrefix, Constants.SUFFIX_TRACE_FILE,
                            false, false);
                    traceSystem.setFileName(file);
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, filePrefix);
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

    private SessionInfo si;

    @Override
    public void setSessionInfo(SessionInfo si) {
        this.si = si;
    }

    @Override
    public SessionInfo getSessionInfo() {
        return si;
    }

    @Override
    public <T> AsyncCallback<T> createSingleThreadCallback() {
        if (DbException.ASSERT) {
            DbException.assertTrue(
                    getScheduler() == null || getScheduler() == SchedulerThread.currentScheduler());
        }
        return AsyncCallback.createSingleThreadCallback();
    }
}
