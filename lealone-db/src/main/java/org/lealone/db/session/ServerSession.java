/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.session;

import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceSystem;
import org.lealone.common.util.ExpiringMap;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.db.Command;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.DataHandler;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ManualCloseable;
import org.lealone.db.Procedure;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.User;
import org.lealone.db.constraint.Constraint;
import org.lealone.db.index.Index;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.result.Result;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLob;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.sql.ParsedSQLStatement;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLCommand;
import org.lealone.sql.SQLParser;
import org.lealone.sql.SQLStatement;
import org.lealone.storage.lob.LobStorage;
import org.lealone.storage.page.IPage;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionListener;

/**
 * A session represents an embedded database connection. When using the server
 * mode, this object resides on the server side and communicates with a
 * Session object on the client side.
 *
 * @author H2 Group
 * @author zhh
 */
public class ServerSession extends SessionBase {
    /**
     * The prefix of generated identifiers. It may not have letters, because
     * they are case sensitive.
     */
    private static final String SYSTEM_IDENTIFIER_PREFIX = "_";

    private Database database;
    private ConnectionInfo connectionInfo;
    private final User user;
    private final int id;
    private final ArrayList<DbObjectLock> locks = new ArrayList<>();
    private Random random;
    private int lockTimeout;
    private Value lastIdentity = ValueLong.get(0);
    private Value lastScopeIdentity = ValueLong.get(0);
    private HashMap<String, Table> localTempTables;
    private HashMap<String, Index> localTempTableIndexes;
    private HashMap<String, Constraint> localTempTableConstraints;
    private int throttle;
    private long lastThrottle;
    private PreparedSQLStatement currentCommand;
    private int currentCommandSavepointId;
    private int currentCommandLockIndex; // 执行当前命令过程中会占用各种锁，记下在locks中的开始位置
    private boolean allowLiterals;
    private String currentSchemaName;
    private String[] schemaSearchPath;
    private Trace trace;
    private DataHandler currentDataHandler;
    private HashMap<Integer, DataHandler> dataHandlers;
    private HashMap<String, ValueLob> unlinkLobMapAtCommit;
    private HashMap<String, ValueLob> unlinkLobMapAtRollback;
    private boolean containsLargeObject;
    private int systemIdentifier;
    private HashMap<String, Procedure> procedures;
    private boolean autoCommitAtTransactionEnd;
    private long cancelAt;
    private final long sessionStart = System.currentTimeMillis();
    private long transactionStart;
    private long currentCommandStart;
    private HashMap<String, Value> variables;
    private HashSet<Result> temporaryResults;
    private int queryTimeout;
    private boolean commitOrRollbackDisabled;
    private int modificationId;
    private int objectId;
    private int queryCacheSize;
    private SmallLRUCache<String, PreparedSQLStatement> queryCache;
    private long modificationMetaID = -1;

    private boolean containsDDL;
    private boolean containsDatabaseStatement;

    private Transaction transaction;
    private HashSet<IPage> dirtyPages;

    public ServerSession(Database database, User user, int id) {
        this.database = database;
        this.queryTimeout = database.getSettings().maxQueryTimeout;
        this.queryCacheSize = database.getSettings().queryCacheSize;
        this.user = user;
        this.id = id;
        this.lockTimeout = database.getSettings().defaultLockTimeout;
        this.currentSchemaName = Constants.SCHEMA_MAIN;
        currentDataHandler = database;
    }

    @Override
    public int getId() {
        return id;
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public void setQueryCacheSize(int queryCacheSize) {
        this.queryCacheSize = queryCacheSize;
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
            if (value instanceof ValueLob)
                value = ((ValueLob) value).link(database, LobStorage.TABLE_ID_SESSION_VARIABLE);
            old = variables.put(name, value);
        }
        if (old != null) {
            // close the old value (in case it is a lob)
            if (old instanceof ValueLob)
                ((ValueLob) old).unlink(database);
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
            table.removeChildrenAndResources(this, null);
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
                index.removeChildrenAndResources(this, null);
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
    public void removeLocalTempTableConstraint(Constraint constraint) {
        if (localTempTableConstraints != null) {
            localTempTableConstraints.remove(constraint.getName());
            synchronized (database) {
                constraint.removeChildrenAndResources(this, null);
            }
        }
    }

    public User getUser() {
        return user;
    }

    @Override
    public int getLockTimeout() {
        return lockTimeout;
    }

    public void setLockTimeout(int lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

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
        return parser.parse(sql).prepare();
    }

    public PreparedSQLStatement prepareStatementLocal(String sql) {
        SQLParser parser = database.createParser(this);
        return parser.parse(sql).prepare();
    }

    @Override
    public synchronized SQLCommand createSQLCommand(String sql, int fetchSize) {
        return prepareStatement(sql, fetchSize);
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
                } else {
                    ps = queryCache.get(sql);
                    if (ps != null && ps.canReuse()) {
                        ps.reuse();
                        return ps;
                    }
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
        if (fetchSize != -1)
            ps.setFetchSize(fetchSize);
        return ps;
    }

    private short executingStatements;

    public void startCurrentCommand(PreparedSQLStatement statement) {
        if (executingStatements++ == 0) {
            currentCommand = statement;
            if (queryTimeout > 0) {
                long now = System.currentTimeMillis();
                currentCommandStart = now;
                cancelAt = now + queryTimeout;
            }
            currentCommandSavepointId = getTransaction().getSavepointId();
            if (locks.isEmpty())
                currentCommandLockIndex = 0;
            else
                currentCommandLockIndex = locks.size() - 1;
        }
        // 在一个事务中可能会执行多条语句，所以记录一下其中有哪些类型
        if (statement.isDatabaseStatement())
            containsDatabaseStatement = true;
        else if (statement.isDDL())
            containsDDL = true;
    }

    private void closeCurrentCommand() {
        // 关闭后一些DML语句才可以重用
        if (currentCommand != null) {
            currentCommand.close();
            currentCommand = null;
        }
    }

    public <T> void stopCurrentCommand(PreparedSQLStatement statement,
            AsyncHandler<AsyncResult<T>> asyncHandler, AsyncResult<T> asyncResult) {
        // 执行rollback命令时executingStatements会置0，然后再执行stopCurrentCommand
        // 此时executingStatements不需要再减了
        if (executingStatements > 0 && --executingStatements > 0) {
            statement.close();
            // 增加记录时，如果触发TableAnalyzer，下面的语句会导致错误
            // setStatus(SessionStatus.STATEMENT_RUNNING); // 切回RUNNING状态
            return;
        }
        boolean asyncCommit = false;
        boolean isCommitCommand = currentCommand != null
                && currentCommand.getType() == SQLStatement.COMMIT;
        closeTemporaryResults();
        closeCurrentCommand();
        if (asyncResult != null && asyncHandler != null) {
            if (isAutoCommit() || isCommitCommand) {
                asyncCommit = true;
                // 不阻塞当前线程，异步提交事务，等到事务日志写成功后再给客户端返回语句的执行结果
                asyncCommit(() -> asyncHandler.handle(asyncResult));
            } else {
                // 当前语句是在一个手动提交的事务中进行，提前给客户端返回语句的执行结果
                asyncHandler.handle(asyncResult);
            }
        } else {
            if (isAutoCommit() || isCommitCommand) {
                // 阻塞当前线程，可能需要等事务日志写完为止
                commit();
            }
        }
        // asyncCommit执行完后才能把YieldableCommand置null，否则会导致部分响应无法发送
        if (!asyncCommit) {
            setYieldableCommand(null);
        }
    }

    public void rollbackCurrentCommand() {
        rollbackTo(currentCommandSavepointId);
        int size = locks.size();
        if (currentCommandLockIndex < size) {
            // 只解除当前语句拥有的锁
            ArrayList<DbObjectLock> list = new ArrayList<>(locks);
            for (int i = currentCommandLockIndex; i < size; i++) {
                DbObjectLock lock = list.get(i);
                lock.unlock(this, false);
                locks.remove(lock);
            }
        }
    }

    public void asyncCommit(Runnable asyncTask) {
        if (transaction != null) {
            beforeCommit();
            transaction.asyncCommit(() -> {
                commitFinal();
                if (asyncTask != null)
                    asyncTask.run();
            });
        } else {
            // 包含子查询的场景
            if (asyncTask != null)
                asyncTask.run();
        }
    }

    /**
     * Commit the current transaction. If the statement was not a data
     * definition statement, and if there are temporary tables that should be
     * dropped or truncated at commit, this is done as well.
     */
    public void commit() {
        if (transaction != null) {
            beforeCommit();
            transaction.commit();
            commitFinal();
        }
    }

    private void beforeCommit() {
        addLobTask();
        checkCommitRollback();
        checkDataModification();
        sessionStatus = SessionStatus.TRANSACTION_COMMITTING;
    }

    private void checkCommitRollback() {
        if (commitOrRollbackDisabled && locks.size() > 0) {
            throw DbException.get(ErrorCode.COMMIT_ROLLBACK_NOT_ALLOWED);
        }
    }

    private void checkDataModification() {
        // 手动提交时，如果更新了数据，让缓存失效，这样其他还没结束的事务就算开启了缓存也能读到新数据
        if (!isAutoCommit() && transaction.getSavepointId() > 0)
            database.getNextModificationDataId();
    }

    private void endTransaction() {
        containsDDL = false;
        containsDatabaseStatement = false;
        transaction.wakeUpWaitingTransactions();
        transactionStart = 0;
        transaction = null;
    }

    private void unlinkLob(HashMap<String, ValueLob> lobMap) {
        if (lobMap != null) {
            for (ValueLob v : lobMap.values()) {
                v.unlink(v.getHandler());
                v.close();
            }
        }
    }

    private void addLobTask() {
        if (containsLargeObject) {
            unlinkLob(unlinkLobMapAtCommit);
            unlinkLobMapAtCommit = null;
            unlinkLobMapAtRollback = null;
            containsLargeObject = false;

            if (dataHandlers != null) {
                HashMap<Integer, DataHandler> dHandlers = dataHandlers;
                dataHandlers = null;
                transaction.addLobTask(() -> {
                    for (DataHandler dh : dHandlers.values())
                        dh.getLobStorage().save();
                });
            }
        }
    }

    private void commitFinal() {
        if (!containsDDL) {
            // do not clean the temp tables if the last command was a create/drop
            cleanTempTables(false);
            if (autoCommitAtTransactionEnd) {
                autoCommit = true;
                autoCommitAtTransactionEnd = false;
            }
        }
        unlockAll(true);
        markDirtyPages();
        endTransaction();
        yieldableCommand = null;
        sessionStatus = SessionStatus.TRANSACTION_NOT_START;
    }

    /**
     * Fully roll back the current transaction.
     */
    public void rollback() {
        if (transaction == null)
            return;
        checkCommitRollback();
        transaction.rollback();
        cleanTempTables(false);
        unlockAll(false);
        endTransaction();

        if (containsLargeObject) {
            unlinkLob(unlinkLobMapAtRollback);
            unlinkLobMapAtCommit = null;
            unlinkLobMapAtRollback = null;
            containsLargeObject = false;
        }
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
        executingStatements = 0; // 回滚时置0，否则出现锁超时异常时会导致严重错误
        yieldableCommand = null;
        sessionStatus = SessionStatus.TRANSACTION_NOT_START;
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
    public void addSavepoint(String name) {
        getTransaction().addSavepoint(name);
    }

    /**
     * Undo all operations back to the log position of the given savepoint.
     *
     * @param name the savepoint name
     */
    public void rollbackToSavepoint(String name) {
        if (transaction != null) {
            checkCommitRollback();
            transaction.rollbackToSavepoint(name);
        }
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
                closeAllCache();
                cleanTempTables(true);
                database.removeSession(this);
            } finally {
                super.close();
            }
        }
    }

    /**
     * Add a lock for the given DbObject. The object is unlocked on commit or rollback.
     *
     * @param lock the lock that is locked
     */
    public void addLock(DbObjectLock lock) {
        if (SysProperties.CHECK) {
            if (locks.indexOf(lock) >= 0) {
                DbException.throwInternalError();
            }
        }
        locks.add(lock);
    }

    private void unlockAll(boolean succeeded) {
        if (!locks.isEmpty()) {
            // don't use the enhanced for loop to save memory
            for (int i = 0, size = locks.size(); i < size; i++) {
                DbObjectLock lock = locks.get(i);
                lock.unlock(this, succeeded);
            }
            locks.clear();
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
                        table.removeChildrenAndResources(this, null);
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
            initTraceSystem(connectionInfo, database.getStoragePath());
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
            Connection conn = (Connection) jdbcConnectionClass
                    .getConstructor(Session.class, String.class, String.class)
                    .newInstance(this, user, url);
            return conn;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public DataHandler getDataHandler() {
        return currentDataHandler;
    }

    public void setDataHandler(DataHandler dh) {
        currentDataHandler = dh;
    }

    public void addDataHandler(int tableId, DataHandler dataHandler) {
        if (dataHandlers == null) {
            dataHandlers = new HashMap<>();
        }
        dataHandlers.put(tableId, dataHandler);
    }

    public void unlinkAtCommit(ValueLob v) {
        if (SysProperties.CHECK && !v.isLinked()) {
            DbException.throwInternalError();
        }
        if (unlinkLobMapAtCommit == null) {
            unlinkLobMapAtCommit = new HashMap<>();
        }
        unlinkLobMapAtCommit.put(v.toString(), v);
        containsLargeObject = true;
    }

    public void unlinkAtRollback(ValueLob v) {
        if (SysProperties.CHECK && !v.isLinked()) {
            DbException.throwInternalError();
        }
        if (unlinkLobMapAtRollback == null) {
            unlinkLobMapAtRollback = new HashMap<>();
        }
        unlinkLobMapAtRollback.put(v.toString(), v);
        containsLargeObject = true;
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
        return id;
    }

    @Override
    public String toString() {
        return "#" + id + " (user: " + user.getName() + ")";
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

    public DbObjectLock[] getLocks() {
        // copy the data without synchronizing
        int size = locks.size();
        ArrayList<DbObjectLock> copy = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            try {
                copy.add(locks.get(i));
            } catch (Exception e) {
                // ignore
                break;
            }
        }
        DbObjectLock[] list = new DbObjectLock[copy.size()];
        copy.toArray(list);
        return list;
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

    public Transaction getTransaction() {
        if (transaction != null)
            return transaction;

        Transaction transaction = database.getTransactionEngine().beginTransaction(autoCommit,
                transactionIsolationLevel);
        transaction.setSession(this);

        sessionStatus = SessionStatus.TRANSACTION_NOT_COMMIT;
        this.transaction = transaction;
        return transaction;
    }

    public SQLParser getParser() {
        return database.createParser(this);
    }

    private SessionStatus sessionStatus = SessionStatus.TRANSACTION_NOT_START;

    @Override
    public SessionStatus getStatus() {
        if (isExclusiveMode())
            return SessionStatus.EXCLUSIVE_MODE;
        return sessionStatus;
    }

    @Override
    public void setStatus(SessionStatus sessionStatus) {
        this.sessionStatus = sessionStatus;
    }

    public static class YieldableCommand {

        private final int packetId;
        private final PreparedSQLStatement.Yieldable<?> yieldable;
        private final int sessionId;

        public YieldableCommand(int packetId, PreparedSQLStatement.Yieldable<?> yieldable,
                int sessionId) {
            this.packetId = packetId;
            this.yieldable = yieldable;
            this.sessionId = sessionId;
        }

        public int getPacketId() {
            return packetId;
        }

        public int getSessionId() {
            return sessionId;
        }

        public Session getSession() {
            return yieldable.getSession();
        }

        public int getPriority() {
            return yieldable.getPriority();
        }

        public void run() {
            yieldable.run();
        }

        public void stop() {
            yieldable.stop();
        }
    }

    public static interface TimeoutListener {
        void onTimeout(YieldableCommand c, Throwable e);
    }

    private YieldableCommand yieldableCommand;

    public void setYieldableCommand(YieldableCommand yieldableCommand) {
        this.yieldableCommand = yieldableCommand;
    }

    public YieldableCommand getYieldableCommand() {
        return yieldableCommand;
    }

    public YieldableCommand getYieldableCommand(boolean checkTimeout, TimeoutListener timeoutListener) {
        if (yieldableCommand == null)
            return null;

        // session处于以下状态时不会被当成候选的对象
        switch (getStatus()) {
        case WAITING:
            if (checkTimeout) {
                checkTransactionTimeout(timeoutListener);
            }
        case TRANSACTION_COMMITTING:
        case EXCLUSIVE_MODE:
        case STATEMENT_RUNNING:
            return null;
        }
        return yieldableCommand;
    }

    private void checkTransactionTimeout(TimeoutListener timeoutListener) {
        Transaction t = transaction;
        if (t != null) {
            try {
                t.checkTimeout();
            } catch (Throwable e) {
                if (timeoutListener != null)
                    timeoutListener.onTimeout(yieldableCommand, e);
                rollback();
            }
        }
    }

    private TransactionListener transactionListener;

    public TransactionListener getTransactionListener() {
        return transactionListener;
    }

    public void setTransactionListener(TransactionListener transactionListener) {
        this.transactionListener = transactionListener;
    }

    @Override
    public void setNetworkTimeout(int milliseconds) {
        if (connectionInfo != null)
            connectionInfo.setNetworkTimeout(milliseconds);
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

    private ExpiringMap<Integer, ManualCloseable> cache; // 缓存PreparedStatement和结果集
    private SmallLRUCache<String, InputStream> lobCache; // 大多数情况下都不使用lob，所以延迟初始化

    private void closeAllCache() {
        if (cache != null) {
            cache.close();
            cache = null;
        }
        if (lobCache != null) {
            for (InputStream in : lobCache.values()) {
                try {
                    in.close();
                } catch (Throwable t) {
                    // ignore
                }
            }
            lobCache = null;
        }
    }

    public void setCache(ExpiringMap<Integer, ManualCloseable> cache) {
        this.cache = cache;
    }

    public void addCache(Integer k, ManualCloseable v) {
        cache.put(k, v);
    }

    public ManualCloseable getCache(Integer k) {
        return cache.get(k);
    }

    public ManualCloseable removeCache(Integer k, boolean ifAvailable) {
        return cache.remove(k, ifAvailable);
    }

    public SmallLRUCache<String, InputStream> getLobCache() {
        if (lobCache == null) {
            lobCache = SmallLRUCache.newInstance(Math.max(SysProperties.SERVER_CACHED_OBJECTS,
                    SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));
        }
        return lobCache;
    }

    private int transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED; // 默认是读已提交级别

    public int getTransactionIsolationLevel() {
        return transactionIsolationLevel;
    }

    public void setTransactionIsolationLevel(String transactionIsolationLevel) {
        switch (transactionIsolationLevel.toUpperCase()) {
        case "READ_COMMITTED":
            this.transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
            break;
        case "REPEATABLE_READ":
            this.transactionIsolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
            break;
        case "SERIALIZABLE":
            this.transactionIsolationLevel = Connection.TRANSACTION_SERIALIZABLE;
            break;
        case "READ_UNCOMMITTED":
            this.transactionIsolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
            break;
        default:
            throw DbException.getInvalidValueException("transaction isolation level",
                    transactionIsolationLevel);
        }
    }

    public void setTransactionIsolationLevel(int transactionIsolationLevel) {
        switch (transactionIsolationLevel) {
        case Connection.TRANSACTION_READ_COMMITTED:
        case Connection.TRANSACTION_REPEATABLE_READ:
        case Connection.TRANSACTION_SERIALIZABLE:
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            break;
        default:
            throw DbException.getInvalidValueException("transaction isolation level",
                    transactionIsolationLevel);
        }
        this.transactionIsolationLevel = transactionIsolationLevel;
    }

    private String valueVectorFactoryName;

    public String getValueVectorFactoryName() {
        return valueVectorFactoryName;
    }

    public void setValueVectorFactoryName(String valueVectorFactoryName) {
        this.valueVectorFactoryName = valueVectorFactoryName;
    }

    private int expressionCompileThreshold;

    public int getExpressionCompileThreshold() {
        return expressionCompileThreshold;
    }

    public void setExpressionCompileThreshold(int expressionCompileThreshold) {
        this.expressionCompileThreshold = expressionCompileThreshold;
    }

    private String olapOperatorFactoryName;

    public String getOlapOperatorFactoryName() {
        return olapOperatorFactoryName;
    }

    public void setOlapOperatorFactoryName(String olapOperatorFactoryName) {
        this.olapOperatorFactoryName = olapOperatorFactoryName;
    }

    private int olapThreshold;

    public int getOlapThreshold() {
        return olapThreshold;
    }

    public void setOlapThreshold(int olapThreshold) {
        this.olapThreshold = olapThreshold;
    }

    private int olapBatchSize;

    public int getOlapBatchSize() {
        return olapBatchSize;
    }

    public void setOlapBatchSize(int olapBatchSize) {
        this.olapBatchSize = olapBatchSize;
    }

    public Map<String, String> getSettings() {
        Map<String, String> settings = new LinkedHashMap<>(SessionSetting.values().length);
        for (SessionSetting setting : SessionSetting.values()) {
            Object v = "";
            switch (setting) {
            case LOCK_TIMEOUT:
                v = lockTimeout;
                break;
            case QUERY_TIMEOUT:
                v = queryTimeout;
                break;
            case SCHEMA:
                v = currentSchemaName;
                break;
            case SCHEMA_SEARCH_PATH:
                v = schemaSearchPath;
                break;
            case VARIABLE:
                continue;
            case THROTTLE:
                v = throttle;
                break;
            case TRANSACTION_ISOLATION_LEVEL:
                v = transactionIsolationLevel;
                break;
            case VALUE_VECTOR_FACTORY_NAME:
                v = valueVectorFactoryName;
                break;
            case EXPRESSION_COMPILE_THRESHOLD:
                v = expressionCompileThreshold;
                break;
            case OLAP_OPERATOR_FACTORY_NAME:
                v = olapOperatorFactoryName;
                break;
            case OLAP_THRESHOLD:
                v = olapThreshold;
                break;
            case OLAP_BATCH_SIZE:
                v = olapBatchSize;
                break;
            }
            settings.put(setting.name(), v == null ? "null" : v.toString());
        }
        return settings;
    }

    public void clearQueryCache() {
        if (queryCache != null)
            queryCache.clear();
    }

    @Override
    public boolean isQueryCommand() {
        return currentCommand != null && currentCommand.isQuery();
    }

    @Override
    public boolean isForUpdate() {
        return currentCommand != null && (!currentCommand.isQuery() || currentCommand.isForUpdate());
    }

    private boolean undoLogEnabled = true;

    @Override
    public boolean isUndoLogEnabled() {
        return undoLogEnabled;
    }

    public void setUndoLogEnabled(boolean enabled) {
        undoLogEnabled = enabled;
    }

    @Override
    public void addDirtyPage(IPage page) {
        if (dirtyPages == null)
            dirtyPages = new HashSet<>();
        dirtyPages.add(page);
    }

    private void markDirtyPages() {
        if (dirtyPages != null) {
            for (IPage page : dirtyPages)
                page.markDirtyBottomUp();
            dirtyPages = null;
        }
    }

    @Override
    public long getCurrentTid() {
        return transaction != null ? transaction.getTransactionId() : 0;
    }
}
