/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.New;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.StorageMap;

/**
 * The base class for both client and server sessions.
 */
public abstract class SessionBase implements Session {

    protected ArrayList<String> sessionState;
    protected boolean sessionStateChanged;
    private boolean sessionStateUpdating;

    protected String replicationName;
    protected boolean local;
    protected AtomicInteger nextId = new AtomicInteger(0);

    protected Callable<?> callable;

    /**
     * Re-create the session state using the stored sessionState list.
     */
    protected void recreateSessionState() {
        if (sessionState != null && sessionState.size() > 0) {
            sessionStateUpdating = true;
            try {
                for (String sql : sessionState) {
                    PreparedStatement ps = prepareStatement(sql, Integer.MAX_VALUE);
                    ps.update();
                }
            } finally {
                sessionStateUpdating = false;
                sessionStateChanged = false;
            }
        }
    }

    /**
     * Read the session state if necessary.
     */
    public void readSessionState() {
        if (!sessionStateChanged || sessionStateUpdating) {
            return;
        }
        sessionStateChanged = false;
        sessionState = New.arrayList();
        Command c = prepareCommand("SELECT * FROM INFORMATION_SCHEMA.SESSION_STATE", Integer.MAX_VALUE);
        Result result = c.query(0, false);
        while (result.next()) {
            Value[] row = result.currentRow();
            sessionState.add(row[1].getString());
        }
    }

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
    public void setCallable(Callable<?> callable) {
        this.callable = callable;
    }

    @Override
    public Callable<?> getCallable() {
        return callable;
    }

    @Override
    public void prepareCommit(boolean ddl) {
    }

}
