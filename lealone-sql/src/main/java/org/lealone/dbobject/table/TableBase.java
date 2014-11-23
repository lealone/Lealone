/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.dbobject.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.lealone.api.DatabaseEventListener;
import org.lealone.command.ddl.Analyze;
import org.lealone.command.ddl.CreateTableData;
import org.lealone.constant.Constants;
import org.lealone.constant.ErrorCode;
import org.lealone.constant.SysProperties;
import org.lealone.dbobject.DbObject;
import org.lealone.dbobject.SchemaObject;
import org.lealone.dbobject.constraint.Constraint;
import org.lealone.dbobject.constraint.ConstraintReferential;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.Index;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.index.MultiVersionIndex;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.message.Trace;
import org.lealone.result.Row;
import org.lealone.util.MathUtils;
import org.lealone.util.New;
import org.lealone.util.StatementBuilder;
import org.lealone.util.StringUtils;
import org.lealone.value.CompareMode;
import org.lealone.value.DataType;
import org.lealone.value.Value;

/**
 * The base class of a regular table, or a user defined table.
 *
 * @author Thomas Mueller
 * @author Sergi Vladykin
 */
public abstract class TableBase extends Table {
    protected final boolean globalTemporary;
    protected boolean containsLargeObject;
    protected long rowCount;

    protected Index scanIndex;
    protected final ArrayList<Index> indexes = New.arrayList();

    private volatile Session lockExclusive;
    private HashSet<Session> lockShared;
    private final Trace traceLock;
    protected long lastModificationId;
    private int changesSinceAnalyze;
    private int nextAnalyze;
    private Column rowIdColumn;

    /**
     * True if one thread ever was waiting to lock this table. This is to avoid
     * calling notifyAll if no session was ever waiting to lock this table. If
     * set, the flag stays. In theory, it could be reset, however not sure when.
     */
    private boolean waitForLock;

    public TableBase(CreateTableData data) {
        super(data.schema, data.id, data.tableName, data.persistIndexes, data.persistData);
        this.tableEngine = data.tableEngine;
        this.globalTemporary = data.globalTemporary;

        setTemporary(data.temporary);
        initColumns(data.columns);
        nextAnalyze = database.getSettings().analyzeAuto;
        this.isHidden = data.isHidden;
        for (Column col : getColumns()) {
            if (DataType.isLargeObject(col.getType())) {
                containsLargeObject = true;
            }
        }

        traceLock = database.getTrace(Trace.LOCK);
        lockShared = New.hashSet();
    }

    protected void initColumns(ArrayList<Column> columns) {
        Column[] cols = new Column[columns.size()];
        columns.toArray(cols);
        setColumns(cols);
    }

    public boolean getContainsLargeObject() {
        return containsLargeObject;
    }

    /**
     * Set the row count of this table.
     *
     * @param count the row count
     */
    public void setRowCount(long count) {
        this.rowCount = count;
    }

    @Override
    public String getDropSQL() {
        return "DROP TABLE IF EXISTS " + getSQL() + " CASCADE";
    }

    @Override
    public String getCreateSQL() {
        StatementBuilder buff = new StatementBuilder("CREATE ");
        if (isTemporary()) {
            if (isGlobalTemporary()) {
                buff.append("GLOBAL ");
            } else {
                buff.append("LOCAL ");
            }
            buff.append("TEMPORARY ");
        } else if (isPersistIndexes()) {
            buff.append("CACHED ");
        } else {
            buff.append("MEMORY ");
        }
        buff.append("TABLE ");
        if (isHidden) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append("(\n    ");
        for (Column column : columns) {
            buff.appendExceptFirst(",\n    ");
            buff.append(column.getCreateSQL());
        }
        buff.append("\n)");
        if (tableEngine != null) {
            String d = getDatabase().getSettings().defaultTableEngine;
            if (d == null || !tableEngine.endsWith(d)) {
                buff.append("\nENGINE \"");
                buff.append(tableEngine);
                buff.append('\"');
            }
        }
        if (!isPersistIndexes() && !isPersistData()) {
            buff.append("\nNOT PERSISTENT");
        }
        if (isHidden) {
            buff.append("\nHIDDEN");
        }
        return buff.toString();
    }

    @Override
    public void close(Session session) {
        for (Index index : indexes) {
            index.close(session);
        }
    }

    @Override
    public boolean isGlobalTemporary() {
        return globalTemporary;
    }

    /**
     * Read the given row.
     *
     * @param session the session
     * @param key unique key
     * @return the row
     */
    public Row getRow(Session session, long key) {
        return scanIndex.getRow(session, key);
    }

    @Override
    public void addRow(Session session, Row row) {
        lastModificationId = database.getNextModificationDataId();
        if (database.isMultiVersion()) {
            row.setSessionId(session.getId());
        }
        int i = 0;
        try {
            for (int size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                index.add(session, row);
                checkRowCount(session, index, 1);
            }
            rowCount++;
        } catch (Throwable e) {
            try {
                while (--i >= 0) {
                    Index index = indexes.get(i);
                    index.remove(session, row);
                    checkRowCount(session, index, 0);
                }
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means there is something wrong
                // with the database
                trace.error(e2, "could not undo operation");
                throw e2;
            }
            DbException de = DbException.convert(e);
            if (de.getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                for (int j = 0; j < indexes.size(); j++) {
                    Index index = indexes.get(j);
                    if (index.getIndexType().isUnique() && index instanceof MultiVersionIndex) {
                        MultiVersionIndex mv = (MultiVersionIndex) index;
                        if (mv.isUncommittedFromOtherSession(session, row)) {
                            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, index.getName());
                        }
                    }
                }
            }
            throw de;
        }
        analyzeIfRequired(session);
    }

    @Override
    public void commit(short operation, Row row) {
        lastModificationId = database.getNextModificationDataId();
        for (int i = 0, size = indexes.size(); i < size; i++) {
            Index index = indexes.get(i);
            index.commit(operation, row);
        }
    }

    public void checkRowCount(Session session, Index index, int offset) {
        if (SysProperties.CHECK && !database.isMultiVersion()) {
            long rc = index.getRowCount(session);
            if (rc != rowCount + offset) {
                DbException.throwInternalError("rowCount expected " + (rowCount + offset) //
                        + " got " + rc + " " + getName() + "." + index.getName());
            }
        }
    }

    @Override
    public Index getScanIndex(Session session) {
        return indexes.get(0);
    }

    @Override
    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public abstract Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment);

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public long getRowCount(Session session) {
        if (database.isMultiVersion()) {
            return getScanIndex(session).getRowCount(session);
        }
        return rowCount;
    }

    @Override
    public void removeRow(Session session, Row row) {
        if (database.isMultiVersion()) {
            if (row.isDeleted()) {
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, getName());
            }
            int old = row.getSessionId();
            int newId = session.getId();
            if (old == 0) {
                row.setSessionId(newId);
            } else if (old != newId) {
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, getName());
            }
        }
        lastModificationId = database.getNextModificationDataId();
        int i = indexes.size() - 1;
        try {
            for (; i >= 0; i--) {
                Index index = indexes.get(i);
                index.remove(session, row);
                checkRowCount(session, index, -1);
            }
            rowCount--;
        } catch (Throwable e) {
            try {
                while (++i < indexes.size()) {
                    Index index = indexes.get(i);
                    index.add(session, row);
                    checkRowCount(session, index, 0);
                }
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means there is something wrong
                // with the database
                trace.error(e2, "could not undo operation");
                throw e2;
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public void truncate(Session session) {
        lastModificationId = database.getNextModificationDataId();
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        rowCount = 0;
        changesSinceAnalyze = 0;
    }

    protected void analyzeIfRequired(Session session) {
        if (nextAnalyze == 0 || nextAnalyze > changesSinceAnalyze++) {
            return;
        }
        changesSinceAnalyze = 0;
        int n = 2 * nextAnalyze;
        if (n > 0) {
            nextAnalyze = n;
        }
        int rows = session.getDatabase().getSettings().analyzeSample;
        Analyze.analyzeTable(session, this, rows, false);
    }

    @Override
    public boolean isLockedExclusivelyBy(Session session) {
        return lockExclusive == session;
    }

    @Override
    public void lock(Session session, boolean exclusive, boolean force) {
        int lockMode = database.getLockMode();
        if (lockMode == Constants.LOCK_MODE_OFF) {
            return;
        }
        if (!force && database.isMultiVersion()) {
            // MVCC: update, delete, and insert use a shared lock.
            // Select doesn't lock except when using FOR UPDATE and
            // the system property lealone.selectForUpdateMvcc
            // is not enabled
            if (exclusive) {
                exclusive = false;
            } else {
                if (lockExclusive == null) {
                    return;
                }
            }
        }
        if (lockExclusive == session) {
            return;
        }
        synchronized (database) {
            try {
                doLock(session, lockMode, exclusive);
            } finally {
                session.setWaitForLock(null);
            }
        }
    }

    private void doLock(Session session, int lockMode, boolean exclusive) {
        traceLock(session, exclusive, "requesting for");
        // don't get the current time unless necessary
        long max = 0;
        boolean checkDeadlock = false;
        while (true) {
            if (lockExclusive == session) {
                return;
            }
            if (exclusive) {
                if (lockExclusive == null) {
                    if (lockShared.isEmpty()) {
                        traceLock(session, exclusive, "added for");
                        session.addLock(this);
                        lockExclusive = session;
                        return;
                    } else if (lockShared.size() == 1 && lockShared.contains(session)) {
                        traceLock(session, exclusive, "add (upgraded) for ");
                        lockExclusive = session;
                        return;
                    }
                }
            } else {
                if (lockExclusive == null) {
                    if (lockMode == Constants.LOCK_MODE_READ_COMMITTED) {
                        if (!database.isMultiThreaded() && !database.isMultiVersion()) {
                            // READ_COMMITTED: a read lock is acquired,
                            // but released immediately after the operation
                            // is complete.
                            // When allowing only one thread, no lock is
                            // required.
                            // Row level locks work like read committed.
                            return;
                        }
                    }
                    if (!lockShared.contains(session)) {
                        traceLock(session, exclusive, "ok");
                        session.addLock(this);
                        lockShared.add(session);
                    }
                    return;
                }
            }
            session.setWaitForLock(this);
            if (checkDeadlock) {
                ArrayList<Session> sessions = checkDeadlock(session, null, null);
                if (sessions != null) {
                    throw DbException.get(ErrorCode.DEADLOCK_1, getDeadlockDetails(sessions));
                }
            } else {
                // check for deadlocks from now on
                checkDeadlock = true;
            }
            long now = System.currentTimeMillis();
            if (max == 0) {
                // try at least one more time
                max = now + session.getLockTimeout();
            } else if (now >= max) {
                traceLock(session, exclusive, "timeout after " + session.getLockTimeout());
                throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, getName());
            }
            try {
                traceLock(session, exclusive, "waiting for");
                if (database.getLockMode() == Constants.LOCK_MODE_TABLE_GC) {
                    for (int i = 0; i < 20; i++) {
                        long free = Runtime.getRuntime().freeMemory();
                        System.gc();
                        long free2 = Runtime.getRuntime().freeMemory();
                        if (free == free2) {
                            break;
                        }
                    }
                }
                // don't wait too long so that deadlocks are detected early
                long sleep = Math.min(Constants.DEADLOCK_CHECK, max - now);
                if (sleep == 0) {
                    sleep = 1;
                }
                waitForLock = true;
                database.wait(sleep);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private static String getDeadlockDetails(ArrayList<Session> sessions) {
        StringBuilder buff = new StringBuilder();
        for (Session s : sessions) {
            Table lock = s.getWaitForLock();
            buff.append("\nSession ").append(s.toString()).append(" is waiting to lock ").append(lock.toString())
                    .append(" while locking ");
            int i = 0;
            for (Table t : s.getLocks()) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(t.toString());
                if (t instanceof TableBase) {
                    if (((TableBase) t).lockExclusive == s) {
                        buff.append(" (exclusive)");
                    } else {
                        buff.append(" (shared)");
                    }
                }
            }
            buff.append('.');
        }
        return buff.toString();
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

    /**
     * Create a row from the values.
     *
     * @param data the value list
     * @return the row
     */
    public static Row createRow(Value[] data) {
        return new Row(data, Row.MEMORY_CALCULATE);
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

    @Override
    public String toString() {
        return getSQL();
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public void checkSupportAlter() {
        // ok
    }

    @Override
    public boolean canTruncate() {
        if (getCheckForeignKeyConstraints() && database.getReferentialIntegrity()) {
            ArrayList<Constraint> constraints = getConstraints();
            if (constraints != null) {
                for (int i = 0, size = constraints.size(); i < size; i++) {
                    Constraint c = constraints.get(i);
                    if (!(c.getConstraintType().equals(Constraint.REFERENTIAL))) {
                        continue;
                    }
                    ConstraintReferential ref = (ConstraintReferential) c;
                    if (ref.getRefTable() == this) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public String getTableType() {
        return Table.TABLE;
    }

    @Override
    public long getMaxDataModificationId() {
        return lastModificationId;
    }

    @Override
    public long getRowCountApproximation() {
        return scanIndex.getRowCountApproximation();
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    @Override
    public long getDiskSpaceUsed() {
        return scanIndex.getDiskSpaceUsed();
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public Column getRowIdColumn() {
        if (rowIdColumn == null) {
            rowIdColumn = new Column(Column.ROWID, Value.LONG);
            rowIdColumn.setTable(this, -1);
        }
        return rowIdColumn;
    }

    protected void rebuildIfNeed(Session session, Index index, String indexName) {
        if (index.needRebuild() && rowCount > 0) {
            try {
                Index scan = getScanIndex(session);
                long remaining = scan.getRowCount(session);
                long total = remaining;
                Cursor cursor = scan.find(session, null, null);
                long i = 0;
                int bufferSize = (int) Math.min(rowCount, Constants.DEFAULT_MAX_MEMORY_ROWS);
                ArrayList<Row> buffer = New.arrayList(bufferSize);
                String n = getName() + ":" + index.getName();
                int t = MathUtils.convertLongToInt(total);
                while (cursor.next()) {
                    database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n, MathUtils.convertLongToInt(i++), t);
                    Row row = cursor.get();
                    buffer.add(row);
                    if (buffer.size() >= bufferSize) {
                        addRowsToIndex(session, buffer, index);
                    }
                    remaining--;
                }
                addRowsToIndex(session, buffer, index);
                if (SysProperties.CHECK && remaining != 0) {
                    DbException.throwInternalError("rowcount remaining=" + remaining + " " + getName());
                }
            } catch (DbException e) {
                getSchema().freeUniqueName(indexName);
                try {
                    index.remove(session);
                } catch (DbException e2) {
                    // this could happen, for example on failure in the storage
                    // but if that is not the case it means
                    // there is something wrong with the database
                    trace.error(e2, "could not remove index");
                    throw e2;
                }
                throw e;
            }
        }
    }

    private static void addRowsToIndex(Session session, ArrayList<Row> list, Index index) {
        final Index idx = index;
        Collections.sort(list, new Comparator<Row>() {
            @Override
            public int compare(Row r1, Row r2) {
                return idx.compareRows(r1, r2);
            }
        });
        for (Row row : list) {
            index.add(session, row);
        }
        list.clear();
    }
}
