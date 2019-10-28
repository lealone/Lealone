/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceSystem;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.User;
import org.lealone.db.constraint.Constraint;
import org.lealone.db.index.Index;
import org.lealone.db.index.StandardPrimaryIndex;
import org.lealone.db.result.Result;
import org.lealone.db.result.Row;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.sql.ParsedSQLStatement;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLCommand;
import org.lealone.sql.SQLParser;
import org.lealone.storage.DistributedStorageMap;
import org.lealone.storage.LobStorage;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;

/**
 * A session represents an embedded database connection. When using the server
 * mode, this object resides on the server side and communicates with a
 * Session object on the client side.
 */
public class ServerSession extends SessionBase implements Transaction.Validator {
    /**
     * The prefix of generated identifiers. It may not have letters, because
     * they are case sensitive.
     */
    private static final String SYSTEM_IDENTIFIER_PREFIX = "_";
    private static int nextSerialId;

    private final int serialId = nextSerialId++;
    private Database database;
    private ConnectionInfo connectionInfo;
    private final User user;
    private final int id;
    private final ArrayList<Table> locks = new ArrayList<>();
    private Random random;
    private int lockTimeout;
    private Value lastIdentity = ValueLong.get(0);
    private Value lastScopeIdentity = ValueLong.get(0);
    private HashMap<String, Table> localTempTables;
    private HashMap<String, Index> localTempTableIndexes;
    private HashMap<String, Constraint> localTempTableConstraints;
    private int throttle;
    private long lastThrottle;
    private Command currentCommand;
    private boolean allowLiterals;
    private String currentSchemaName;
    private String[] schemaSearchPath;
    private Trace trace;
    private HashMap<String, Value> unlinkLobMap;
    private int systemIdentifier;
    private HashMap<String, Procedure> procedures;
    private boolean autoCommitAtTransactionEnd;
    private volatile long cancelAt;
    private final long sessionStart = System.currentTimeMillis();
    private long transactionStart;
    private long currentCommandStart;
    private HashMap<String, Value> variables;
    private HashSet<Result> temporaryResults;
    private int queryTimeout;
    private boolean commitOrRollbackDisabled;
    private Table waitForLock;
    private Thread waitForLockThread;
    private int modificationId;
    private int objectId;
    private final int queryCacheSize;
    private SmallLRUCache<String, PreparedSQLStatement> queryCache;
    private long modificationMetaID = -1;

    private boolean containsDDL;
    private boolean containsDatabaseStatement;

    public ServerSession(Database database, User user, int id) {
        this.database = database;
        this.queryTimeout = database.getSettings().maxQueryTimeout;
        this.queryCacheSize = database.getSettings().queryCacheSize;
        this.user = user;
        this.id = id;
        Setting setting = database.findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_LOCK_TIMEOUT));
        this.lockTimeout = setting == null ? Constants.INITIAL_LOCK_TIMEOUT : setting.getIntValue();
        this.currentSchemaName = Constants.SCHEMA_MAIN;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public boolean setCommitOrRollbackDisabled(boolean x) {
        boolean old = commitOrRollbackDisabled;
        commitOrRollbackDisabled = x;
        return old;
    }

    private void initVariables() {
        if (variables == null) {
            variables = database.newStringMap();
        }
    }

    /**
     * Set the value of the given variable for this session.
     *
     * @param name the name of the variable (may not be null)
     * @param value the new value (may not be null)
     */
    public void setVariable(String name, Value value) {
        initVariables();
        modificationId++;
        Value old;
        if (value == ValueNull.INSTANCE) {
            old = variables.remove(name);
        } else {
            // link LOB values, to make sure we have our own object
            value = value.link(database, LobStorage.TABLE_ID_SESSION_VARIABLE);
            old = variables.put(name, value);
        }
        if (old != null) {
            // close the old value (in case it is a lob)
            old.unlink(database);
            old.close();
        }
    }

    /**
     * Get the value of the specified user defined variable. This method always
     * returns a value; it returns ValueNull.INSTANCE if the variable doesn't
     * exist.
     *
     * @param name the variable name
     * @return the value, or NULL
     */
    public Value getVariable(String name) {
        initVariables();
        Value v = variables.get(name);
        return v == null ? ValueNull.INSTANCE : v;
    }

    /**
     * Get the list of variable names that are set for this session.
     *
     * @return the list of names
     */
    public String[] getVariableNames() {
        if (variables == null) {
            return new String[0];
        }
        String[] list = new String[variables.size()];
        variables.keySet().toArray(list);
        return list;
    }

    /**
     * Get the local temporary table if one exists with that name, or null if
     * not.
     *
     * @param name the table name
     * @return the table, or null
     */
    public Table findLocalTempTable(String name) {
        if (localTempTables == null) {
            return null;
        }
        return localTempTables.get(name);
    }

    public ArrayList<Table> getLocalTempTables() {
        if (localTempTables == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(localTempTables.values());
    }

    /**
     * Add a local temporary table to this session.
     *
     * @param table the table to add
     * @throws DbException if a table with this name already exists
     */
    public void addLocalTempTable(Table table) {
        if (localTempTables == null) {
            localTempTables = database.newStringMap();
        }
        if (localTempTables.get(table.getName()) != null) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, table.getSQL());
        }
        modificationId++;
        localTempTables.put(table.getName(), table);
    }

    /**
     * Drop and remove the given local temporary table from this session.
     *
     * @param table the table
     */
    public void removeLocalTempTable(Table table) {
        modificationId++;
        localTempTables.remove(table.getName());
        synchronized (database) {
            table.removeChildrenAndResources(this);
        }
    }

    /**
     * Get the local temporary index if one exists with that name, or null if
     * not.
     *
     * @param name the table name
     * @return the table, or null
     */
    public Index findLocalTempTableIndex(String name) {
        if (localTempTableIndexes == null) {
            return null;
        }
        return localTempTableIndexes.get(name);
    }

    public HashMap<String, Index> getLocalTempTableIndexes() {
        if (localTempTableIndexes == null) {
            return new HashMap<>();
        }
        return localTempTableIndexes;
    }

    /**
     * Add a local temporary index to this session.
     *
     * @param index the index to add
     * @throws DbException if a index with this name already exists
     */
    public void addLocalTempTableIndex(Index index) {
        if (localTempTableIndexes == null) {
            localTempTableIndexes = database.newStringMap();
        }
        if (localTempTableIndexes.get(index.getName()) != null) {
            throw DbException.get(ErrorCode.INDEX_ALREADY_EXISTS_1, index.getSQL());
        }
        localTempTableIndexes.put(index.getName(), index);
    }

    /**
     * Drop and remove the given local temporary index from this session.
     *
     * @param index the index
     */
    public void removeLocalTempTableIndex(Index index) {
        if (localTempTableIndexes != null) {
            localTempTableIndexes.remove(index.getName());
            synchronized (database) {
                index.removeChildrenAndResources(this);
            }
        }
    }

    /**
     * Get the local temporary constraint if one exists with that name, or
     * null if not.
     *
     * @param name the constraint name
     * @return the constraint, or null
     */
    public Constraint findLocalTempTableConstraint(String name) {
        if (localTempTableConstraints == null) {
            return null;
        }
        return localTempTableConstraints.get(name);
    }

    /**
     * Get the map of constraints for all constraints on local, temporary
     * tables, if any. The map's keys are the constraints' names.
     *
     * @return the map of constraints, or null
     */
    public HashMap<String, Constraint> getLocalTempTableConstraints() {
        if (localTempTableConstraints == null) {
            return new HashMap<>();
        }
        return localTempTableConstraints;
    }

    /**
     * Add a local temporary constraint to this session.
     *
     * @param constraint the constraint to add
     * @throws DbException if a constraint with the same name already exists
     */
    public void addLocalTempTableConstraint(Constraint constraint) {
        if (localTempTableConstraints == null) {
            localTempTableConstraints = database.newStringMap();
        }
        String name = constraint.getName();
        if (localTempTableConstraints.get(name) != null) {
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, constraint.getSQL());
        }
        localTempTableConstraints.put(name, constraint);
    }

    /**
     * Drop and remove the given local temporary constraint from this session.
     *
     * @param constraint the constraint
     */
    void removeLocalTempTableConstraint(Constraint constraint) {
        if (localTempTableConstraints != null) {
            localTempTableConstraints.remove(constraint.getName());
            synchronized (database) {
                constraint.removeChildrenAndResources(this);
            }
        }
    }

    public User getUser() {
        return user;
    }

    public int getLockTimeout() {
        return lockTimeout;
    }

    public void setLockTimeout(int lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    private Boolean local;

    public void setLocal(boolean local) {
        this.local = local;
    }

    @Override
    public boolean isLocal() {
        return (local != null && local.booleanValue()) || !database.isShardingMode() || connectionInfo == null
                || connectionInfo.isEmbedded();
    }

    @Override
    public ParsedSQLStatement parseStatement(String sql) {
        return database.createParser(this).parse(sql);
    }

    /**
     * Parse and prepare the given SQL statement. This method also checks the rights.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public PreparedSQLStatement prepareStatement(String sql) {
        return prepareStatement(sql, false);
    }

    /**
     * Parse and prepare the given SQL statement.
     *
     * @param sql the SQL statement
     * @param rightsChecked true if the rights have already been checked
     * @return the prepared statement
     */
    public PreparedSQLStatement prepareStatement(String sql, boolean rightsChecked) {
        SQLParser parser = database.createParser(this);
        parser.setRightsChecked(rightsChecked);
        PreparedSQLStatement p = parser.parse(sql).prepare();
        p.setLocal(isLocal());
        return p;
    }

    public PreparedSQLStatement prepareStatementLocal(String sql) {
        SQLParser parser = database.createParser(this);
        PreparedSQLStatement p = parser.parse(sql).prepare();
        p.setLocal(true);
        return p;
    }

    @Override
    public synchronized SQLCommand createSQLCommand(String sql, int fetchSize) {
        return prepareStatement(sql, fetchSize);
    }

    @Override
    public StorageCommand createStorageCommand() {
        return new ServerStorageCommand(this);
    }

    /**
     * Parse and prepare the given SQL statement.
     * This method also checks if the connection has been closed.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    @Override
    public synchronized SQLCommand prepareSQLCommand(String sql, int fetchSize) {
        return prepareStatement(sql, fetchSize);
    }

    @Override
    public PreparedSQLStatement prepareStatement(String sql, int fetchSize) {
        if (closed) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "session closed");
        }
        PreparedSQLStatement ps;
        if (queryCacheSize > 0) {
            if (queryCache == null) {
                queryCache = SmallLRUCache.newInstance(queryCacheSize);
                modificationMetaID = database.getModificationMetaId();
            } else {
                long newModificationMetaID = database.getModificationMetaId();
                if (newModificationMetaID != modificationMetaID) {
                    queryCache.clear();
                    modificationMetaID = newModificationMetaID;
                }
                ps = queryCache.get(sql);
                if (ps != null && ps.canReuse()) {
                    ps.reuse();
                    return ps;
                }
            }
        }
        SQLParser parser = database.createParser(this);
        ps = parser.parse(sql).prepare();
        if (queryCache != null) {
            if (ps.isCacheable()) {
                queryCache.put(sql, ps);
            }
        }
        ps.setLocal(isLocal());
        if (fetchSize != -1)
            ps.setFetchSize(fetchSize);

        return ps;
    }

    @Override
    public Database getDatabase() {
        return database;
    }

    private boolean prepared;
    private String allLocalTransactionNames;

    @Override
    public void prepareCommit() {
        prepared = true;
        if (transaction != null) {
            transaction.setStatus(Transaction.STATUS_COMMITTING);
            sessionStatus = SessionStatus.COMMITTING_TRANSACTION;
            transaction.prepareCommit();
        } else {
            // 在手动提交模式下执行了COMMIT语句，然后再手动提交事务，
            // 此时transaction为null，但是runnable不为null
            if (runnable != null)
                runnable.run();
        }
    }

    public void prepareCommit(String allLocalTransactionNames) {
        prepared = true;
        this.allLocalTransactionNames = allLocalTransactionNames;
        if (transaction != null) {
            transaction.setStatus(Transaction.STATUS_COMMITTING);
            sessionStatus = SessionStatus.COMMITTING_TRANSACTION;
            transaction.prepareCommit(allLocalTransactionNames);
        }
    }

    public void commit() {
        commit(null);
    }

    /**
     * Commit the current transaction. If the statement was not a data
     * definition statement, and if there are temporary tables that should be
     * dropped or truncated at commit, this is done as well.
     */
    @Override
    public void commit(String allLocalTransactionNames) {
        if (prepared) {
            prepared = false;
            allLocalTransactionNames = this.allLocalTransactionNames;
        }
        checkCommitRollback();
        transactionStart = 0;

        if (transaction != null) {
            // 避免重复commit
            Transaction transaction = this.transaction;
            this.transaction = null;
            if (allLocalTransactionNames == null)
                transaction.commit();
            else
                transaction.commit(allLocalTransactionNames);

            endTransaction();
        }
        if (!containsDDL) {
            // do not clean the temp tables if the last command was a
            // create/drop
            cleanTempTables(false);
            if (autoCommitAtTransactionEnd) {
                autoCommit = true;
                autoCommitAtTransactionEnd = false;
            }
        }
        if (unlinkLobMap != null && unlinkLobMap.size() > 0) {
            // need to flush the transaction log, because we can't unlink lobs
            // if the commit record is not written
            database.flush();
            for (Value v : unlinkLobMap.values()) {
                v.unlink(database);
                v.close();
            }
            unlinkLobMap = null;
        }
        unlockAll();
        clean();
        releaseSessionCache();
        sessionStatus = SessionStatus.NO_TRANSACTION;
    }

    private void endTransaction() {
        if (!isRoot)
            setAutoCommit(true);

        containsDDL = false;
        containsDatabaseStatement = false;
        setReplicationName(null);
    }

    private void checkCommitRollback() {
        if (commitOrRollbackDisabled && locks.size() > 0) {
            throw DbException.get(ErrorCode.COMMIT_ROLLBACK_NOT_ALLOWED);
        }
    }

    /**
     * Fully roll back the current transaction.
     */
    @Override
    public void rollback() {
        checkCommitRollback();
        if (transaction != null) {
            Transaction transaction = this.transaction;
            this.transaction = null;
            transaction.rollback();
            endTransaction();
        }
        cleanTempTables(false);
        unlockAll();
        if (autoCommitAtTransactionEnd) {
            autoCommit = true;
            autoCommitAtTransactionEnd = false;
        }

        if (containsDatabaseStatement) {
            LealoneDatabase.getInstance().copy();
            containsDatabaseStatement = false;
        }

        if (containsDDL) {
            Database db = this.database;
            db.copy();
            containsDDL = false;
        }

        clean();
        releaseSessionCache();
    }

    /**
     * Partially roll back the current transaction.
     *
     * @param index the position to which should be rolled back 
     */
    public void rollbackTo(int index) {
        if (transaction != null) {
            checkCommitRollback();
            transaction.rollbackToSavepoint(index);
        }
    }

    /**
     * Create a savepoint that is linked to the current log position.
     *
     * @param name the savepoint name
     */
    @Override
    public void addSavepoint(String name) {
        getTransaction().addSavepoint(name);
    }

    /**
     * Undo all operations back to the log position of the given savepoint.
     *
     * @param name the savepoint name
     */
    @Override
    public void rollbackToSavepoint(String name) {
        if (transaction != null) {
            checkCommitRollback();
            transaction.rollbackToSavepoint(name);
        }
    }

    public int getId() {
        return id;
    }

    @Override
    public int getSessionId() {
        return id;
    }

    @Override
    public void cancel() {
        cancelAt = System.currentTimeMillis();
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                database.checkPowerOff();
                cleanTempTables(true);
                database.removeSession(this);
            } finally {
                super.close();
            }
        }
    }

    /**
     * Add a lock for the given table. The object is unlocked on commit or
     * rollback.
     *
     * @param table the table that is locked
     */
    public void addLock(Table table) {
        if (SysProperties.CHECK) {
            if (locks.indexOf(table) >= 0) {
                DbException.throwInternalError();
            }
        }
        locks.add(table);
    }

    /**
     * Unlock just this table.
     *
     * @param t the table to unlock
     */
    public void unlock(Table t) {
        locks.remove(t);
    }

    private void unlockAll() {
        if (!locks.isEmpty()) {
            // don't use the enhanced for loop to save memory
            for (int i = 0, size = locks.size(); i < size; i++) {
                Table t = locks.get(i);
                t.unlock(this);
            }
            locks.clear();
        }
    }

    public ArrayList<ServerSession> checkDeadlock() {
        if (locks.isEmpty())
            return null;
        return locks.get(0).checkDeadlock(this, null, null);
    }

    private void releaseSessionCache() {
        if (!sessionCache.isEmpty()) {
            for (Session s : sessionCache.values()) {
                s.setParentTransaction(null);
                SessionPool.release(s);
            }

            sessionCache.clear();
        }
    }

    private void cleanTempTables(boolean closeSession) {
        if (localTempTables != null && localTempTables.size() > 0) {
            synchronized (database) {
                for (Table table : new ArrayList<>(localTempTables.values())) {
                    if (closeSession || table.getOnCommitDrop()) {
                        modificationId++;
                        table.setModified();
                        localTempTables.remove(table.getName());
                        table.removeChildrenAndResources(this);
                    } else if (table.getOnCommitTruncate()) {
                        table.truncate(this);
                    }
                }
            }
        }
    }

    public Random getRandom() {
        if (random == null) {
            random = new Random();
        }
        return random;
    }

    public Trace getTrace() {
        if (trace != null && !closed) {
            return trace;
        }
        String traceModuleName = "jdbc[" + id + "]";
        if (closed) {
            return new TraceSystem().getTrace(traceModuleName);
        }
        if (connectionInfo != null) {
            initTraceSystem(connectionInfo);
        } else {
            traceSystem = database.getTraceSystem();
        }
        if (traceSystem == null)
            trace = Trace.NO_TRACE;
        else
            trace = traceSystem.getTrace(traceModuleName);
        return trace;
    }

    public void setLastIdentity(Value last) {
        this.lastIdentity = last;
        this.lastScopeIdentity = last;
    }

    public Value getLastIdentity() {
        return lastIdentity;
    }

    public void setLastScopeIdentity(Value last) {
        this.lastScopeIdentity = last;
    }

    public Value getLastScopeIdentity() {
        return lastScopeIdentity;
    }

    public void setThrottle(int throttle) {
        this.throttle = throttle;
    }

    /**
     * Wait for some time if this session is throttled (slowed down).
     */
    public void throttle() {
        if (currentCommandStart == 0) {
            currentCommandStart = System.currentTimeMillis();
        }
        if (throttle == 0) {
            return;
        }
        long time = System.currentTimeMillis();
        if (lastThrottle + Constants.THROTTLE_DELAY > time) {
            return;
        }
        lastThrottle = time + throttle;
        try {
            Thread.sleep(throttle);
        } catch (Exception e) {
            // ignore InterruptedException
        }
    }

    /**
     * Set the current command of this session. This is done just before
     * executing the statement.
     *
     * @param command the command
     */
    public void setCurrentCommand(PreparedSQLStatement statement) {
        this.currentCommand = statement;
        if (queryTimeout > 0 && statement != null) {
            long now = System.currentTimeMillis();
            currentCommandStart = now;
            cancelAt = now + queryTimeout;
        }
        // 在一个事务中可能会执行多条语句，所以记录一下其中有哪些类型
        // 注意，在执行完当前语句后会再调用一次，把currentCommand设为null，所以这里要加一下判断
        if (statement != null) {
            if (statement.isDatabaseStatement())
                containsDatabaseStatement = true;
            else if (statement.isDDL())
                containsDDL = true;
        }
    }

    /**
     * Check if the current transaction is canceled by calling
     * Statement.cancel() or because a session timeout was set and expired.
     *
     * @throws DbException if the transaction is canceled
     */
    public void checkCanceled() {
        throttle();
        if (cancelAt == 0) {
            return;
        }
        long time = System.currentTimeMillis();
        if (time >= cancelAt) {
            cancelAt = 0;
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    /**
     * Get the cancel time.
     *
     * @return the time or 0 if not set
     */
    public long getCancel() {
        return cancelAt;
    }

    public Command getCurrentCommand() {
        return currentCommand;
    }

    public long getCurrentCommandStart() {
        return currentCommandStart;
    }

    public boolean getAllowLiterals() {
        return allowLiterals;
    }

    public void setAllowLiterals(boolean b) {
        this.allowLiterals = b;
    }

    public void setCurrentSchema(Schema schema) {
        modificationId++;
        this.currentSchemaName = schema.getName();
    }

    public String getCurrentSchemaName() {
        return currentSchemaName;
    }

    /**
     * Create an internal connection. This connection is used when initializing
     * triggers, and when calling user defined functions.
     *
     * @param columnList if the url should be 'jdbc:lealone:columnlist:connection'
     * @return the internal connection
     */
    public Connection createConnection(boolean columnList) {
        String url;
        if (columnList) {
            url = Constants.CONN_URL_COLUMNLIST;
        } else {
            url = Constants.CONN_URL_INTERNAL;
        }
        return createConnection(getUser().getName(), url);
    }

    public Connection createConnection(String user, String url) {
        try {
            Class<?> jdbcConnectionClass = Class.forName(Constants.REFLECTION_JDBC_CONNECTION);
            Connection conn = (Connection) jdbcConnectionClass.getConstructor(Session.class, String.class, String.class)
                    .newInstance(this, user, url);
            return conn;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public DataHandler getDataHandler() {
        return database;
    }

    /**
     * Remember that the given LOB value must be un-linked (disconnected from
     * the table) at commit.
     *
     * @param v the value
     */
    public void unlinkAtCommit(Value v) {
        if (SysProperties.CHECK && !v.isLinked()) {
            DbException.throwInternalError();
        }
        if (unlinkLobMap == null) {
            unlinkLobMap = new HashMap<>();
        }
        unlinkLobMap.put(v.toString(), v);
    }

    /**
     * Do not unlink this LOB value at commit any longer.
     *
     * @param v the value
     */
    public void unlinkAtCommitStop(Value v) {
        if (unlinkLobMap != null) {
            unlinkLobMap.remove(v.toString());
        }
    }

    /**
     * Get the next system generated identifiers. The identifier returned does
     * not occur within the given SQL statement.
     *
     * @param sql the SQL statement
     * @return the new identifier
     */
    public String getNextSystemIdentifier(String sql) {
        String identifier;
        do {
            identifier = SYSTEM_IDENTIFIER_PREFIX + systemIdentifier++;
        } while (sql.indexOf(identifier) >= 0);
        return identifier;
    }

    /**
     * Add a procedure to this session.
     *
     * @param procedure the procedure to add
     */
    public void addProcedure(Procedure procedure) {
        if (procedures == null) {
            procedures = database.newStringMap();
        }
        procedures.put(procedure.getName(), procedure);
    }

    /**
     * Remove a procedure from this session.
     *
     * @param name the name of the procedure to remove
     */
    public void removeProcedure(String name) {
        if (procedures != null) {
            procedures.remove(name);
        }
    }

    /**
     * Get the procedure with the given name, or null
     * if none exists.
     *
     * @param name the procedure name
     * @return the procedure or null
     */
    public Procedure getProcedure(String name) {
        if (procedures == null) {
            return null;
        }
        return procedures.get(name);
    }

    public void setSchemaSearchPath(String[] schemas) {
        modificationId++;
        this.schemaSearchPath = schemas;
    }

    public String[] getSchemaSearchPath() {
        return schemaSearchPath;
    }

    @Override
    public int hashCode() {
        return serialId;
    }

    @Override
    public String toString() {
        return "#" + serialId + " (user: " + user.getName() + ")";
    }

    /**
     * Begin a transaction.
     */
    public void begin() {
        autoCommitAtTransactionEnd = true;
        autoCommit = false;
    }

    public long getSessionStart() {
        return sessionStart;
    }

    public long getTransactionStart() {
        if (transactionStart == 0) {
            transactionStart = System.currentTimeMillis();
        }
        return transactionStart;
    }

    public Table[] getLocks() {
        // copy the data without synchronizing
        int size = locks.size();
        ArrayList<Table> copy = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            try {
                copy.add(locks.get(i));
            } catch (Exception e) {
                // ignore
                break;
            }
        }
        Table[] list = new Table[copy.size()];
        copy.toArray(list);
        return list;
    }

    /**
     * Wait if the exclusive mode has been enabled for another session. This
     * method returns as soon as the exclusive mode has been disabled.
     */
    public void waitIfExclusiveModeEnabled() {
        while (true) {
            if (!isExclusiveMode())
                break;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public boolean isExclusiveMode() {
        ServerSession exclusive = database.getExclusiveSession();
        if (exclusive == null || exclusive == this) {
            return false;
        }
        if (Thread.holdsLock(exclusive)) {
            // if another connection is used within the connection
            return false;
        }
        return true;
    }

    /**
     * Remember the result set and close it as soon as the transaction is
     * committed (if it needs to be closed). This is done to delete temporary
     * files as soon as possible, and free object ids of temporary tables.
     *
     * @param result the temporary result set
     */
    public void addTemporaryResult(Result result) {
        if (!result.needToClose()) {
            return;
        }
        if (temporaryResults == null) {
            temporaryResults = new HashSet<>();
        }
        if (temporaryResults.size() < 100) {
            // reference at most 100 result sets to avoid memory problems
            temporaryResults.add(result);
        }
    }

    /**
     * Close all temporary result set. This also deletes all temporary files
     * held by the result sets.
     */
    public void closeTemporaryResults() {
        if (temporaryResults != null) {
            for (Result result : temporaryResults) {
                result.close();
            }
            temporaryResults = null;
        }
    }

    public void setQueryTimeout(int queryTimeout) {
        int max = database.getSettings().maxQueryTimeout;
        if (max != 0 && (max < queryTimeout || queryTimeout == 0)) {
            // the value must be at most max
            queryTimeout = max;
        }
        this.queryTimeout = queryTimeout;
        // must reset the cancel at here,
        // otherwise it is still used
        this.cancelAt = 0;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Set the table this session is waiting for, and the thread that is
     * waiting.
     *
     * @param waitForLock the table
     * @param waitForLockThread the current thread (the one that is waiting)
     */
    public void setWaitForLock(Table waitForLock, Thread waitForLockThread) {
        this.waitForLock = waitForLock;
        this.waitForLockThread = waitForLockThread;
    }

    public Table getWaitForLock() {
        return waitForLock;
    }

    public Thread getWaitForLockThread() {
        return waitForLockThread;
    }

    @Override
    public int getModificationId() {
        return modificationId;
    }

    public void setConnectionInfo(ConnectionInfo ci) {
        connectionInfo = ci;
    }

    @Override
    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public Value getTransactionId() {
        if (transaction == null) {
            return ValueNull.INSTANCE;
        }
        return ValueString.get(Long.toString(transaction.getTransactionId()));
    }

    /**
     * Get the next object id.
     *
     * @return the next object id
     */
    public int nextObjectId() {
        return objectId++;
    }

    private boolean isRoot = true; // 分布式事务最开始启动时所在的session就是root session，相当于协调者

    public boolean isRoot() {
        return isRoot;
    }

    @Override
    public void setRoot(boolean isRoot) {
        this.isRoot = isRoot;
    }

    public String getURL(String hostId) {
        if (connectionInfo == null) {
            String dbName = database.getShortName();
            String url = createURL(dbName, hostId);
            connectionInfo = new ConnectionInfo(url, dbName);
            connectionInfo.setUserName(user.getName());
            connectionInfo.setUserPasswordHash(user.getUserPasswordHash());
            return url;
        }
        StringBuilder buff = new StringBuilder();
        String url = connectionInfo.getURL();
        int pos1 = url.indexOf("//") + 2;
        buff.append(url.substring(0, pos1)).append(hostId);

        int pos2 = url.indexOf('/', pos1);
        buff.append(url.substring(pos2));
        return buff.toString();
    }

    private volatile Transaction transaction;

    @Override
    public Transaction getTransaction() {
        return getTransaction(null);
    }

    @Override
    public Transaction getTransaction(PreparedSQLStatement p) {
        if (transaction != null)
            return transaction;

        boolean isShardingMode = isShardingMode();
        Transaction transaction = database.getTransactionEngine().beginTransaction(autoCommit, isShardingMode);
        transaction.setValidator(this);
        transaction.setSession(this);
        transaction.setGlobalTransactionName(replicationName);

        // TODO p != null && !p.isLocal()是否需要？
        if (isRoot && !autoCommit && isShardingMode && p != null && !p.isLocal())
            transaction.setLocal(false);

        sessionStatus = SessionStatus.TRANSACTION_NOT_COMMIT;
        this.transaction = transaction;
        return transaction;
    }

    // 参与本次事务的其他Session
    protected final Map<String, Session> sessionCache = new HashMap<>();

    public Map<String, Session> getSessionCache() {
        return sessionCache;
    }

    public void addSession(String url, Session s) {
        if (transaction != null && !sessionCache.containsKey(url))
            transaction.addParticipant(s);
        sessionCache.put(url, s);
    }

    // 得到的嵌套session会参与当前事务
    @Override
    public Session getNestedSession(String hostAndPort, boolean remote) {
        // 不能直接把hostAndPort当成key，因为每个Session是对应到具体数据库的，所以URL中要包含数据库名
        String url = getURL(hostAndPort);
        Session s = sessionCache.get(url);
        if (s == null) {
            s = SessionPool.getSession(this, url, remote);
            if (transaction != null)
                transaction.addParticipant(s);
            sessionCache.put(url, s);
        }
        return s;
    }

    public Session getSession(String url) {
        return sessionCache.get(url);
    }

    @Override
    public boolean validateTransaction(String localTransactionName) {
        return database.getTransactionEngine().validateTransaction(localTransactionName);
    }

    @Override
    public boolean validate(String localTransactionName) {
        String[] a = localTransactionName.split(":");
        Session s = null;
        try {
            String dbName = getDatabase().getShortName();
            String url = createURL(dbName, a[0], a[1]);
            // 不参与当前事务，所以不用当成当前session的嵌套session
            s = SessionPool.getSession(this, url, true);
            return s.validateTransaction(localTransactionName);
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            SessionPool.release(s);
        }
    }

    @Override
    public boolean validate(String hostAndPort, String localTransactionName) {
        Session s = null;
        try {
            String dbName = getDatabase().getShortName();
            String url = createURL(dbName, hostAndPort);
            // 不参与当前事务，所以不用当成当前session的嵌套session
            s = SessionPool.getSession(this, url, true);
            return s.validateTransaction(localTransactionName);
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            SessionPool.release(s);
        }
    }

    private static String createURL(String dbName, String host, String port) {
        StringBuilder url = new StringBuilder(100);
        url.append(Constants.URL_PREFIX).append(Constants.URL_TCP).append("//");
        url.append(host).append(":").append(port);
        url.append("/").append(dbName);
        return url.toString();
    }

    private static String createURL(String dbName, String hostAndPort) {
        StringBuilder url = new StringBuilder(100);
        url.append(Constants.URL_PREFIX).append(Constants.URL_TCP).append("//");
        url.append(hostAndPort);
        url.append("/").append(dbName);
        return url.toString();
    }

    public SQLParser getParser() {
        return database.createParser(this);
    }

    @Override
    public Session connect(boolean allowRedirect) {
        return this;
    }

    @Override
    public String getURL() {
        return connectionInfo == null ? null : connectionInfo.getURL();
    }

    @Override
    public void commitTransaction(String localTransactionName) {
        commit(localTransactionName);
    }

    @Override
    public void rollbackTransaction() {
        rollback();
    }

    @Override
    public boolean isShardingMode() {
        return database.isShardingMode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public StorageMap<Object, Object> getStorageMap(String mapName) {
        // 数据库可能还没有初始化，这时事务引擎中就找不到对应的Map
        if (!database.isInitialized())
            database.init();
        TransactionEngine transactionEngine = database.getTransactionEngine();
        return (StorageMap<Object, Object>) transactionEngine.getTransactionMap(mapName).getInstance(getTransaction());
    }

    @Override
    public void replicateRootPages(String dbName, ByteBuffer rootPages) {
        Database database = LealoneDatabase.getInstance().getDatabase(dbName);
        if (!database.isInitialized()) {
            database.init();
        }
        int size = rootPages.getInt();
        for (int i = 0; i < size; i++) {
            String mapName = ValueString.type.read(rootPages);
            DistributedStorageMap<?, ?> map = (DistributedStorageMap<?, ?>) database.getStorageMap(mapName);
            map.setRootPage(rootPages);
            if (i == 0) {
                database = database.copy();
            }
        }
    }

    private SessionStatus sessionStatus = SessionStatus.NO_TRANSACTION;

    @Override
    public SessionStatus getStatus() {
        if (isExclusiveMode())
            return SessionStatus.EXCLUSIVE_MODE;
        return sessionStatus;
    }

    private StandardPrimaryIndex lastIndex;
    private Row lastRow;

    public void setLastIndex(StandardPrimaryIndex i) {
        lastIndex = i;
    }

    public void setLastRow(Row r) {
        lastRow = r;
    }

    @Override
    public long getLastRowKey() {
        if (lastRow == null)
            return 0;
        return lastRow.getKey();
    }

    @Override
    public void replicationCommit(long validKey, boolean autoCommit) {
        if (validKey != -1) {
            if (transaction != null) {
                transaction.replicationPrepareCommit(validKey);
            }
            if (lastRow != null) {
                Table table = lastIndex.getTable();
                Row oldRow = lastIndex.getRow(this, validKey);
                // 已经修正过了
                if (oldRow != null && oldRow.getValueList() == lastRow.getValueList()) {
                    if (autoCommit)
                        commit();
                    return;
                }
                if (oldRow != null)
                    table.removeRow(this, oldRow);
                table.removeRow(this, lastRow);

                if (oldRow != null) {
                    oldRow.setKey(lastRow.getKey());
                    table.addRow(this, oldRow);
                }
                lastRow.setKey(validKey);
                table.addRow(this, lastRow);
            }
        }
        if (autoCommit) {
            commit();
        }
    }

    public void copyLastReplicationStatusTo(ServerSession newSession) {
        newSession.lastRow = lastRow;
        newSession.lastIndex = lastIndex;
    }

    private void clean() {
        lastRow = null;
        lastIndex = null;
    }

    private byte[] lobMacSalt;

    @Override
    public void setLobMacSalt(byte[] lobMacSalt) {
        this.lobMacSalt = lobMacSalt;
    }

    @Override
    public byte[] getLobMacSalt() {
        return lobMacSalt;
    }

    @Override
    public String getUserName() {
        return user.getName();
    }

    @Override
    public int getNetworkTimeout() {
        return connectionInfo != null ? connectionInfo.getNetworkTimeout() : -1;
    }

    @Override
    public void cancelStatement(int statementId) {
        if (currentCommand != null && currentCommand.getId() == statementId)
            currentCommand.cancel();
    }
}
