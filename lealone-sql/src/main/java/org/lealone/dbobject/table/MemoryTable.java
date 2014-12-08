/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.lealone.dbobject.table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.lealone.api.ErrorCode;
import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.DbObject;
import org.lealone.dbobject.SchemaObject;
import org.lealone.dbobject.index.HashIndex;
import org.lealone.dbobject.index.Index;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.index.MultiVersionIndex;
import org.lealone.dbobject.index.NonUniqueHashIndex;
import org.lealone.dbobject.index.ScanIndex;
import org.lealone.dbobject.index.TreeIndex;
import org.lealone.engine.Session;
import org.lealone.engine.SysProperties;
import org.lealone.message.DbException;
import org.lealone.message.Trace;
import org.lealone.util.New;

public class MemoryTable extends TableBase {
    private HashSet<Session> lockShared;
    private final Trace traceLock;

    private volatile Session lockExclusive;
    /**
     * True if one thread ever was waiting to lock this table. This is to avoid
     * calling notifyAll if no session was ever waiting to lock this table. If
     * set, the flag stays. In theory, it could be reset, however not sure when.
     */
    private boolean waitForLock;

    public MemoryTable(CreateTableData data) {
        super(data);
        scanIndex = new ScanIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(false));
        indexes.add(scanIndex);

        traceLock = database.getTrace(Trace.LOCK);
        lockShared = New.hashSet();
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
                column.setPrimaryKey(true);
            }
        }
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);
        }
        Index index;
        if (indexType.isHash() && cols.length <= 1) {
            if (indexType.isUnique()) {
                index = new HashIndex(this, indexId, indexName, cols, indexType);
            } else {
                index = new NonUniqueHashIndex(this, indexId, indexName, cols, indexType);
            }
        } else {
            index = new TreeIndex(this, indexId, indexName, cols, indexType);
        }
        if (database.isMultiVersion()) {
            index = new MultiVersionIndex(index, this);
        }
        rebuildIfNeed(session, index, indexName);
        index.setTemporary(isTemporary());
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                database.addSchemaObject(session, index);
            }
        }
        indexes.add(index);
        setModified();
        return index;
    }

    @Override
    public String getTableType() {
        return MemoryTableEngine.NAME + "_" + super.getTableType();
    }

    @Override
    public ArrayList<Session> checkDeadlock(Session session, Session clash, Set<Session> visited) {
        // only one deadlock check at any given time
        synchronized (TableBase.class) {
            if (clash == null) {
                // verification is started
                clash = session;
                visited = New.hashSet();
            } else if (clash == session) {
                // we found a circle where this session is involved
                return New.arrayList();
            } else if (visited.contains(session)) {
                // we have already checked this session.
                // there is a circle, but the sessions in the circle need to
                // find it out themselves
                return null;
            }
            visited.add(session);
            ArrayList<Session> error = null;
            for (Session s : lockShared) {
                if (s == session) {
                    // it doesn't matter if we have locked the object already
                    continue;
                }
                Table t = s.getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(s, clash, visited);
                    if (error != null) {
                        error.add(session);
                        break;
                    }
                }
            }
            if (error == null && lockExclusive != null) {
                Table t = lockExclusive.getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(lockExclusive, clash, visited);
                    if (error != null) {
                        error.add(session);
                    }
                }
            }
            return error;
        }
    }

    private void traceLock(Session session, boolean exclusive, String s) {
        if (traceLock.isDebugEnabled()) {
            traceLock.debug("{0} {1} {2} {3}", session.getId(), exclusive ? "exclusive write lock" : "shared read lock", s,
                    getName());
        }
    }

    @Override
    public boolean isLockedExclusively() {
        return lockExclusive != null;
    }

    @Override
    public void unlock(Session s) {
        if (database != null) {
            traceLock(s, lockExclusive == s, "unlock");
            if (lockExclusive == s) {
                lockExclusive = null;
            }
            if (lockShared.size() > 0) {
                lockShared.remove(s);
            }
            // TODO lock: maybe we need we fifo-queue to make sure nobody
            // starves. check what other databases do
            synchronized (database) {
                if (database.getSessionCount() > 1 && waitForLock) {
                    database.notifyAll();
                }
            }
        }
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        if (containsLargeObject) {
            // unfortunately, the data is gone on rollback
            truncate(session);
            database.getLobStorage().removeAllForTable(getId());
            database.lockMeta(session);
        }
        super.removeChildrenAndResources(session);
        // go backwards because database.removeIndex will call table.removeIndex
        while (indexes.size() > 1) {
            Index index = indexes.get(1);
            if (index.getName() != null) {
                database.removeSchemaObject(session, index);
            }
        }
        if (SysProperties.CHECK) {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.INDEX)) {
                Index index = (Index) obj;
                if (index.getTable() == this) {
                    DbException.throwInternalError("index not dropped: " + index.getName());
                }
            }
        }
        if (scanIndex != null)
            scanIndex.remove(session);
        scanIndex = null;
        lockExclusive = null;
        lockShared = null;
        invalidate();
    }
}
