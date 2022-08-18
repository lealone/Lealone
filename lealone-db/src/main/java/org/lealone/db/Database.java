/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.trace.TraceSystem;
import org.lealone.common.util.BitField;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.TempFileDeleter;
import org.lealone.common.util.Utils;
import org.lealone.db.api.DatabaseEventListener;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.Right;
import org.lealone.db.auth.Role;
import org.lealone.db.auth.User;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexType;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.lock.DbObjectLockImpl;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.SchemaObject;
import org.lealone.db.schema.Sequence;
import org.lealone.db.schema.TriggerObject;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionStatus;
import org.lealone.db.session.SystemSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.CreateTableData;
import org.lealone.db.table.MetaTable;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableView;
import org.lealone.db.util.SourceCompiler;
import org.lealone.db.value.CompareMode;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.sql.SQLEngine;
import org.lealone.sql.SQLParser;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.lob.LobStorage;
import org.lealone.transaction.TransactionEngine;

/**
 * There is one database object per open database.
 *
 * @author H2 Group
 * @author zhh
 */
public class Database implements DataHandler, DbObject {

    /**
     * The default name of the system user. This name is only used as long as
     * there is no administrator user registered.
     */
    private static final String SYSTEM_USER_NAME = "DBA";

    private static enum State {
        CONSTRUCTOR_CALLED, // 刚调用完构造函数阶段，也是默认阶段
        OPENING,
        OPENED,
        STARTING,
        STARTED,
        CLOSING,
        CLOSED,
        POWER_OFF
    }

    private volatile State state = State.CONSTRUCTOR_CALLED;

    private final TransactionalDbObjects[] dbObjectsArray = new TransactionalDbObjects[DbObjectType.TYPES.length];

    // 与users、roles和rights相关的操作都用这个对象进行同步
    private final DbObjectLock authLock = new DbObjectLockImpl(DbObjectType.USER);
    private final DbObjectLock schemasLock = new DbObjectLockImpl(DbObjectType.SCHEMA);
    private final DbObjectLock commentsLock = new DbObjectLockImpl(DbObjectType.COMMENT);
    private final DbObjectLock databasesLock = new DbObjectLockImpl(DbObjectType.DATABASE);

    public DbObjectLock tryExclusiveAuthLock(ServerSession session) {
        if (authLock.tryExclusiveLock(session)) {
            return authLock;
        } else {
            return null;
        }
    }

    public DbObjectLock tryExclusiveSchemaLock(ServerSession session) {
        if (schemasLock.tryExclusiveLock(session)) {
            return schemasLock;
        } else {
            return null;
        }
    }

    public DbObjectLock tryExclusiveCommentLock(ServerSession session) {
        if (commentsLock.tryExclusiveLock(session)) {
            return commentsLock;
        } else {
            return null;
        }
    }

    public DbObjectLock tryExclusiveDatabaseLock(ServerSession session) {
        if (databasesLock.tryExclusiveLock(session)) {
            return databasesLock;
        } else {
            return null;
        }
    }

    private final Set<ServerSession> userSessions = Collections.synchronizedSet(new HashSet<>());
    private LinkedList<ServerSession> waitingSessions;
    private ServerSession exclusiveSession;
    private SystemSession systemSession;
    private User systemUser;
    private Schema mainSchema;
    private Schema infoSchema;
    private volatile boolean infoSchemaMetaTablesInitialized;
    private Role publicRole;

    private int nextSessionId;
    private int nextTempTableId;

    private final BitField objectIds = new BitField();
    private final Object lobSyncObject = new Object();

    private final AtomicLong modificationDataId = new AtomicLong();
    private final AtomicLong modificationMetaId = new AtomicLong();

    private Table meta;
    private String metaStorageEngineName;
    private Index metaIdIndex;

    private TraceSystem traceSystem;
    private Trace trace;

    private boolean readOnly;

    private int closeDelay = -1; // 不关闭
    private DatabaseCloser delayedCloser;
    private boolean deleteFilesOnDisconnect;
    private DatabaseCloser closeOnExit;

    private final TempFileDeleter tempFileDeleter = TempFileDeleter.getInstance();
    private Mode mode = Mode.getDefaultMode();
    private CompareMode compareMode;
    private int compactMode;

    private SourceCompiler compiler;
    private DatabaseEventListener eventListener;
    private DbException backgroundException;
    private QueryStatisticsData queryStatisticsData;

    private final int id;
    private final String name;
    private final Map<String, String> parameters;
    private volatile DbSettings dbSettings;
    private final boolean persistent;

    // 每个数据库只有一个SQL引擎和一个事务引擎
    private final SQLEngine sqlEngine;
    private final TransactionEngine transactionEngine;

    private final ConcurrentHashMap<String, Storage> storages = new ConcurrentHashMap<>();
    private final String storagePath; // 不使用原始的名称，而是用id替换数据库名
    private LobStorage lobStorage;

    private Map<String, String> replicationParameters;
    private Map<String, String> nodeAssignmentParameters;

    private RunMode runMode = RunMode.CLIENT_SERVER;

    private final DbObjectVersionManager dbObjectVersionManager = new DbObjectVersionManager();

    public Database(int id, String name, Map<String, String> parameters) {
        this.id = id;
        this.name = name;
        this.storagePath = getStoragePath();
        if (parameters != null) {
            dbSettings = DbSettings.getInstance(parameters);
            this.parameters = parameters;
        } else {
            dbSettings = DbSettings.getDefaultSettings();
            this.parameters = new CaseInsensitiveMap<>();
        }
        persistent = dbSettings.persistent;
        compareMode = CompareMode.getInstance(null, 0, false);
        if (dbSettings.mode != null) {
            mode = Mode.getInstance(dbSettings.mode);
        }

        String engineName = dbSettings.defaultSQLEngine;
        SQLEngine sqlEngine = PluginManager.getPlugin(SQLEngine.class, engineName);
        if (sqlEngine == null) {
            try {
                sqlEngine = Utils.newInstance(engineName);
                PluginManager.register(sqlEngine);
            } catch (Exception e) {
                e = new RuntimeException("Fatal error: the sql engine '" + engineName + "' not found",
                        e);
                throw DbException.convert(e);
            }
        }
        this.sqlEngine = sqlEngine;

        engineName = dbSettings.defaultTransactionEngine;
        TransactionEngine transactionEngine = PluginManager.getPlugin(TransactionEngine.class,
                engineName);
        if (transactionEngine == null) {
            try {
                transactionEngine = Utils.newInstance(engineName);
                PluginManager.register(transactionEngine);
            } catch (Exception e) {
                e = new RuntimeException(
                        "Fatal error: the transaction engine '" + engineName + "' not found", e);
                throw DbException.convert(e);
            }
        }
        this.transactionEngine = transactionEngine;

        for (DbObjectType type : DbObjectType.TYPES) {
            if (!type.isSchemaObject) {
                dbObjectsArray[type.value] = new TransactionalDbObjects();
            }
        }
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getShortName() {
        return getName();
    }

    public DbSettings getSettings() {
        return dbSettings;
    }

    public boolean setDbSetting(DbSetting key, String value) {
        Map<String, String> newSettings = new CaseInsensitiveMap<>(1);
        newSettings.put(key.getName(), value);
        return setDbSettings(newSettings);
    }

    public boolean setDbSettings(Map<String, String> newSettings) {
        boolean changed = false;
        Map<String, String> oldSettings = new CaseInsensitiveMap<>(dbSettings.getSettings());
        for (Map.Entry<String, String> e : newSettings.entrySet()) {
            String key = e.getKey();
            String newValue = e.getValue();
            String oldValue = oldSettings.get(key);
            if (oldValue == null || !oldValue.equals(newValue)) {
                changed = true;
                oldSettings.put(key, newValue);
            }
        }
        if (changed) {
            dbSettings = DbSettings.getInstance(oldSettings);
            parameters.putAll(newSettings);
            LealoneDatabase.getInstance().updateMeta(systemSession, this);
        }
        return changed;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public SQLEngine getSQLEngine() {
        return sqlEngine;
    }

    public TransactionEngine getTransactionEngine() {
        return transactionEngine;
    }

    public SQLParser createParser(Session session) {
        return sqlEngine.createParser(session);
    }

    public String quoteIdentifier(String identifier) {
        return sqlEngine.quoteIdentifier(identifier);
    }

    public String getDefaultStorageEngineName() {
        return dbSettings.defaultStorageEngine;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void alterParameters(Map<String, String> newParameters) {
        parameters.putAll(newParameters);
    }

    public Map<String, String> getReplicationParameters() {
        return replicationParameters;
    }

    public void setReplicationParameters(Map<String, String> replicationParameters) {
        this.replicationParameters = replicationParameters;
    }

    public Map<String, String> getNodeAssignmentParameters() {
        return nodeAssignmentParameters;
    }

    public void setNodeAssignmentParameters(Map<String, String> nodeAssignmentParameters) {
        this.nodeAssignmentParameters = nodeAssignmentParameters;
    }

    public void setRunMode(RunMode runMode) {
        if (runMode != null) {
            this.runMode = runMode;
        }
    }

    public RunMode getRunMode() {
        return runMode;
    }

    public synchronized Database copy() {
        Database db = new Database(id, name, parameters);
        // 因为每个存储只能打开一次，所以要复用原有存储
        db.storages.putAll(storages);
        db.runMode = runMode;
        db.replicationParameters = replicationParameters;
        db.replicationParameters = nodeAssignmentParameters;
        db.init();
        LealoneDatabase.getInstance().getDatabasesMap().put(name, db);
        for (ServerSession s : userSessions) {
            db.userSessions.add(s);
            s.setDatabase(db);
        }
        return db;
    }

    public boolean isInitialized() {
        return state != State.CONSTRUCTOR_CALLED;
    }

    public synchronized void init() {
        if (state != State.CONSTRUCTOR_CALLED)
            return;
        state = State.OPENING;

        String listener = dbSettings.eventListener;
        if (listener != null) {
            listener = StringUtils.trim(listener, true, true, "'");
            setEventListenerClass(listener);
        }

        initTraceSystem();
        openDatabase();
        addShutdownHook();

        // LealoneDatabase中的表结构是固定的，所以不需要记录表结构修改历史
        if (!isLealoneDatabase()) {
            dbObjectVersionManager.initDbObjectVersionTable(getInternalConnection());
        }

        opened();
        state = State.OPENED;
    }

    private boolean isLealoneDatabase() {
        return LealoneDatabase.ID == id;
    }

    private void initTraceSystem() {
        if (persistent) {
            traceSystem = new TraceSystem(getStoragePath() + Constants.SUFFIX_TRACE_FILE);
            traceSystem.setLevelFile(dbSettings.traceLevelFile);
        } else {
            // 内存数据库不需要写跟踪文件，但是可以输出到控制台
            traceSystem = new TraceSystem();
        }
        traceSystem.setLevelSystemOut(dbSettings.traceLevelSystemOut);
        trace = traceSystem.getTrace(TraceModuleType.DATABASE);
        trace.info("opening {0} (build {1}) (persistent: {2})", name, Constants.BUILD_ID, persistent);
    }

    private void addShutdownHook() {
        if (dbSettings.dbCloseOnExit) {
            try {
                closeOnExit = new DatabaseCloser(this, 0, true);
                ShutdownHookUtils.addShutdownHook(closeOnExit);
            } catch (IllegalStateException | SecurityException e) {
                // shutdown in progress - just don't register the handler
                // (maybe an application wants to write something into a
                // database at shutdown time)
            }
        }
    }

    /**
     * Called after the database has been opened and initialized. This method
     * notifies the event listener if one has been set.
     */
    private void opened() {
        if (eventListener != null) {
            eventListener.opened();
        }
    }

    private void openDatabase() {
        try {
            // 初始化traceSystem后才能做下面这些
            systemUser = new User(this, 0, SYSTEM_USER_NAME, true);
            systemUser.setAdmin(true);

            publicRole = new Role(this, 0, Constants.PUBLIC_ROLE_NAME, true);
            addDatabaseObject(null, publicRole, null);

            mainSchema = new Schema(this, 0, Constants.SCHEMA_MAIN, systemUser, true);
            infoSchema = new Schema(this, -1, "INFORMATION_SCHEMA", systemUser, true);
            addDatabaseObject(null, mainSchema, null);
            addDatabaseObject(null, infoSchema, null);

            systemSession = new SystemSession(this, systemUser, ++nextSessionId);

            // 在一个新事务中打开sys(meta)表
            systemSession.setAutoCommit(false);
            systemSession.getTransaction();
            openMetaTable();
            systemSession.commit();
            systemSession.setAutoCommit(true);
            trace.info("opened {0}", name);
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                e.fillInStackTrace();
            }
            if (traceSystem != null) {
                if (e instanceof SQLException) {
                    SQLException e2 = (SQLException) e;
                    if (e2.getErrorCode() != ErrorCode.DATABASE_ALREADY_OPEN_1) {
                        // only write if the database is not already in use
                        trace.error(e, "opening {0}", name);
                    }
                }
                traceSystem.close();
            }
            closeOpenFilesAndUnlock(false);
            throw DbException.convert(e);
        }
    }

    private void openMetaTable() {
        // sys(meta)表的id固定是0，其他数据库对象的id从1开始
        int sysTableId = 0;
        CreateTableData data = new CreateTableData();
        ArrayList<Column> cols = data.columns;
        Column columnId = new Column("ID", Value.INT);
        columnId.setNullable(false);
        cols.add(columnId);
        cols.add(new Column("TYPE", Value.INT));
        cols.add(new Column("SQL", Value.STRING));
        data.tableName = "SYS";
        data.id = sysTableId;
        data.persistData = persistent;
        data.persistIndexes = persistent;
        data.create = true;
        data.isHidden = true;
        data.session = systemSession;
        data.storageEngineName = metaStorageEngineName = getDefaultStorageEngineName();
        meta = mainSchema.createTable(data);
        objectIds.set(sysTableId); // 此时正处于初始化阶段，只有一个线程在访问，所以不需要同步

        // 创建Delegate索引， 委派给原有的primary index(也就是ScanIndex)
        // Delegate索引不需要生成create语句保存到sys(meta)表的，这里只是把它加到schema和table的相应字段中
        // 这里也没有直接使用sys表的ScanIndex，因为id字段是主键
        IndexColumn[] pkCols = IndexColumn.wrap(new Column[] { columnId });
        IndexType indexType = IndexType.createDelegate();
        metaIdIndex = meta.addIndex(systemSession, "SYS_ID", sysTableId, pkCols, indexType, true, null,
                null);

        // 把sys(meta)表所有的create语句取出来，然后执行它们，在内存中构建出完整的数据库对象
        ArrayList<MetaRecord> records = new ArrayList<>();
        Cursor cursor = metaIdIndex.find(systemSession, null, null);
        while (cursor.next()) {
            MetaRecord rec = new MetaRecord(cursor.get());
            objectIds.set(rec.getId());
            records.add(rec);
        }

        state = State.STARTING;

        // 会按DbObjectType的创建顺序排序
        Collections.sort(records);
        for (MetaRecord rec : records) {
            rec.execute(this, systemSession, eventListener);
        }

        recompileInvalidViews();
        state = State.STARTED;
    }

    private void recompileInvalidViews() {
        boolean recompileSuccessful;
        do {
            recompileSuccessful = false;
            for (Table obj : getAllTablesAndViews(false)) {
                if (obj instanceof TableView) {
                    TableView view = (TableView) obj;
                    if (view.isInvalid()) { // 这里是无效的要recompile
                        view.recompile(systemSession, true);
                        if (!view.isInvalid()) {
                            recompileSuccessful = true;
                        }
                    }
                }
            }
        } while (recompileSuccessful);
        // when opening a database, views are initialized before indexes,
        // so they may not have the optimal plan yet
        // this is not a problem, it is just nice to see the newest plan
        for (Table obj : getAllTablesAndViews(false)) {
            if (obj instanceof TableView) {
                TableView view = (TableView) obj;
                if (!view.isInvalid()) { // 这里是有效的要recompile
                    view.recompile(systemSession, true);
                }
            }
        }
    }

    /**
     * Check if two values are equal with the current comparison mode.
     *
     * @param a the first value
     * @param b the second value
     * @return true if both objects are equal
     */
    public boolean areEqual(Value a, Value b) {
        // can not use equals because ValueDecimal 0.0 is not equal to 0.00.
        return a.compareTo(b, compareMode) == 0;
    }

    /**
     * Compare two values with the current comparison mode. The values may not
     * be of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compare(Value a, Value b) {
        return a.compareTo(b, compareMode);
    }

    /**
     * Compare two values with the current comparison mode. The values must be
     * of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compareTypeSafe(Value a, Value b) {
        return a.compareTypeSafe(b, compareMode);
    }

    public long getModificationDataId() {
        return modificationDataId.get();
    }

    public long getNextModificationDataId() {
        return modificationDataId.incrementAndGet();
    }

    public long getModificationMetaId() {
        return modificationMetaId.get();
    }

    public long getNextModificationMetaId() {
        // if the meta data has been modified, the data is modified as well
        // (because MetaTable returns modificationDataId)
        modificationDataId.incrementAndGet();
        return modificationMetaId.getAndIncrement();
    }

    /**
     * Get the trace object for the given module type.
     *
     * @param traceModuleType the module type
     * @return the trace object
     */
    public Trace getTrace(TraceModuleType traceModuleType) {
        return traceSystem.getTrace(traceModuleType);
    }

    @Override
    public FileStorage openFile(String name, String openMode, boolean mustExist) {
        if (mustExist && !FileUtils.exists(name)) {
            throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        FileStorage fileStorage = FileStorage.open(this, name, openMode, dbSettings.cipher,
                dbSettings.filePasswordHash);
        try {
            fileStorage.init();
        } catch (DbException e) {
            fileStorage.closeSilently();
            throw e;
        }
        return fileStorage;
    }

    /**
     * Check if the file password hash is correct.
     *
     * @param testCipher the cipher algorithm
     * @param testHash the hash code
     * @return true if the cipher algorithm and the password match
     */
    public boolean validateFilePasswordHash(String testCipher, byte[] testHash) {
        if (!StringUtils.equals(testCipher, dbSettings.cipher)) {
            return false;
        }
        return Utils.compareSecure(testHash, dbSettings.filePasswordHash);
    }

    private void initInfoSchemaMetaTables() {
        if (infoSchemaMetaTablesInitialized) {
            return;
        }
        synchronized (infoSchema) {
            if (!infoSchemaMetaTablesInitialized) {
                for (int type = 0, count = MetaTable.getMetaTableTypeCount(); type < count; type++) {
                    MetaTable m = new MetaTable(infoSchema, -1 - type, type);
                    infoSchema.add(null, m, null);
                }
                infoSchemaMetaTablesInitialized = true;
            }
        }
    }

    /**
     * Allocate a new object id.
     *
     * @return the id
     */
    public int allocateObjectId() {
        synchronized (objectIds) {
            int i = objectIds.nextClearBit(0);
            objectIds.set(i);
            return i;
        }
    }

    public void clearObjectId(int id) {
        synchronized (objectIds) {
            objectIds.clear(id);
        }
    }

    public boolean isObjectIdEnabled(int id) {
        synchronized (objectIds) {
            return objectIds.get(id);
        }
    }

    /**
     * Checks if the system table (containing the catalog) is locked.
     *
     * @return true if it is currently locked
     */
    public boolean isSysTableLocked() {
        return meta == null || meta.isLockedExclusively();
    }

    public boolean isSysTableLockedBy(ServerSession session) {
        return meta == null || meta.isLockedExclusivelyBy(session);
    }

    private Cursor getMetaCursor(ServerSession session, int id) {
        SearchRow r = meta.getTemplateSimpleRow(false);
        r.setValue(0, ValueInt.get(id));
        return metaIdIndex.find(session, r, r);
    }

    public Cursor getMetaCursor(ServerSession session) {
        return metaIdIndex.find(session, null, null);
    }

    public Row findMeta(ServerSession session, int id) {
        Cursor cursor = getMetaCursor(session, id);
        if (cursor.next())
            return cursor.get();
        else
            return null;
    }

    public void tryAddMeta(ServerSession session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && isMetaReady() && !obj.isTemporary()) {
            checkWritingAllowed();
            Row r = MetaRecord.getRow(meta, obj);
            meta.addRow(session, r);
        }
    }

    public void tryRemoveMeta(ServerSession session, SchemaObject obj, DbObjectLock lock) {
        if (isMetaReady()) {
            Table t = getDependentTable(obj, null);
            if (t != null && t != obj) {
                throw DbException.get(ErrorCode.CANNOT_DROP_2, obj.getSQL(), t.getSQL());
            }
        }
        tryRemoveMeta(session, obj.getId());
        Comment comment = findComment(session, obj);
        if (comment != null) {
            removeDatabaseObject(session, comment, lock);
        }
    }

    /**
     * Remove the given object from the meta data.
     *
     * @param session the session
     * @param id the id of the object to remove
     */
    private void tryRemoveMeta(ServerSession session, int id) {
        if (id > 0 && isMetaReady()) {
            checkWritingAllowed();
            Row row = findMeta(session, id);
            if (row != null)
                meta.removeRow(session, row);
        }
    }

    /**
     * Update an object in the system table.
     *
     * @param session the session
     * @param obj the database object
     */
    public void updateMeta(ServerSession session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && isMetaReady()) {
            checkWritingAllowed();
            Row oldRow = findMeta(session, id);
            if (oldRow != null) {
                Row newRow = MetaRecord.getRow(meta, obj);
                newRow.setKey(oldRow.getKey());
                Column sqlColumn = meta.getColumn(2);
                meta.updateRow(session, oldRow, newRow, new int[] { sqlColumn.getColumnId() });
            }
        }
    }

    public void updateMetaAndFirstLevelChildren(ServerSession session, DbObject obj) {
        checkWritingAllowed();
        List<? extends DbObject> list = obj.getChildren();
        Comment comment = findComment(session, obj);
        if (comment != null) {
            DbException.throwInternalError();
        }
        updateMeta(session, obj);
        // remember that this scans only one level deep!
        if (list != null) {
            for (DbObject o : list) {
                if (o.getCreateSQL() != null) {
                    updateMeta(session, o);
                }
            }
        }
    }

    /**
     * Add an object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public void addDatabaseObject(ServerSession session, DbObject obj, DbObjectLock lock) {
        TransactionalDbObjects dbObjects = dbObjectsArray[obj.getType().value];

        if (SysProperties.CHECK && dbObjects.containsKey(session, obj.getName())) {
            DbException.throwInternalError("object already exists");
        }

        if (obj.getId() <= 0 || session == null || isStarting()) {
            dbObjects.add(obj);
            return;
        }

        tryAddMeta(session, obj);
        dbObjects.copyOnAdd(session, obj);

        if (lock != null) {
            lock.addHandler(ar -> {
                if (ar.isSucceeded() && ar.getResult()) {
                    dbObjects.commit();
                } else {
                    clearObjectId(obj.getId());
                    dbObjects.rollback();
                }
            });
        }
    }

    /**
     * Remove the object from the database.
     *
     * @param session the session
     * @param obj the object to remove
     */
    public void removeDatabaseObject(ServerSession session, DbObject obj, DbObjectLock lock) {
        checkWritingAllowed();
        String objName = obj.getName();
        DbObjectType type = obj.getType();
        TransactionalDbObjects dbObjects = dbObjectsArray[type.value];
        if (SysProperties.CHECK && !dbObjects.containsKey(session, objName)) {
            DbException.throwInternalError("not found: " + objName);
        }
        if (session == null) {
            dbObjects.remove(objName);
            removeInternal(obj);
            return;
        }
        Comment comment = findComment(session, obj);
        if (comment != null) {
            removeDatabaseObject(session, comment, lock);
        }
        int id = obj.getId();
        obj.removeChildrenAndResources(session, lock);
        tryRemoveMeta(session, id);
        dbObjects.copyOnRemove(session, objName);

        if (lock != null) {
            lock.addHandler(ar -> {
                if (ar.isSucceeded() && ar.getResult()) {
                    dbObjects.commit();
                    removeInternal(obj);
                } else {
                    dbObjects.rollback();
                }
            });
        }
    }

    private void removeInternal(DbObject obj) {
        clearObjectId(obj.getId());
        obj.invalidate();
    }

    /**
     * Rename a database object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public void renameDatabaseObject(ServerSession session, DbObject obj, String newName,
            DbObjectLock lock) {
        checkWritingAllowed();
        DbObjectType type = obj.getType();
        TransactionalDbObjects dbObjects = dbObjectsArray[type.value];
        String oldName = obj.getName();
        if (SysProperties.CHECK) {
            if (!dbObjects.containsKey(session, oldName)) {
                DbException.throwInternalError("not found: " + oldName);
            }
            if (oldName.equals(newName) || dbObjects.containsKey(session, newName)) {
                DbException.throwInternalError("object already exists: " + newName);
            }
        }

        obj.checkRename();
        dbObjects.copyOnRemove(session, oldName);
        obj.rename(newName);
        dbObjects.add(obj);

        lock.addHandler(ar -> {
            if (ar.isSucceeded() && ar.getResult()) {
                dbObjects.commit();
            } else {
                obj.rename(oldName);
                dbObjects.rollback();
            }
        });

        updateMetaAndFirstLevelChildren(session, obj);
    }

    /**
     * Get the comment for the given database object if one exists, or null if
     * not.
     *
     * @param object the database object
     * @return the comment or null
     */
    public Comment findComment(ServerSession session, DbObject object) {
        if (object.getType() == DbObjectType.COMMENT) {
            return null;
        }
        String key = Comment.getKey(object);
        return find(DbObjectType.COMMENT, session, key);
    }

    @SuppressWarnings("unchecked")
    protected <T> HashMap<String, T> getDbObjects(DbObjectType type) {
        return (HashMap<String, T>) dbObjectsArray[type.value].getDbObjects();
    }

    @SuppressWarnings("unchecked")
    protected <T> T find(DbObjectType type, ServerSession session, String name) {
        return (T) dbObjectsArray[type.value].find(session, name);
    }

    /**
     * Get the role if it exists, or null if not.
     *
     * @param roleName the name of the role
     * @return the role or null
     */
    public Role findRole(ServerSession session, String roleName) {
        return find(DbObjectType.ROLE, session, roleName);
    }

    /**
     * Get the schema if it exists, or null if not.
     *
     * @param schemaName the name of the schema
     * @return the schema or null
     */
    public Schema findSchema(ServerSession session, String schemaName) {
        Schema schema = find(DbObjectType.SCHEMA, session, schemaName);
        if (schema == infoSchema) {
            initInfoSchemaMetaTables();
        }
        return schema;
    }

    /**
     * Get the user if it exists, or null if not.
     *
     * @param name the name of the user
     * @return the user or null
     */
    public User findUser(ServerSession session, String name) {
        return find(DbObjectType.USER, session, name);
    }

    /**
     * Get user with the given name. This method throws an exception if the user
     * does not exist.
     *
     * @param name the user name
     * @return the user
     * @throws DbException if the user does not exist
     */
    public User getUser(ServerSession session, String name) {
        User user = findUser(session, name);
        if (user == null) {
            throw DbException.get(ErrorCode.USER_NOT_FOUND_1, name);
        }
        return user;
    }

    public Role getRole(ServerSession session, String name) {
        Role role = findRole(session, name);
        if (role == null) {
            throw DbException.get(ErrorCode.ROLE_NOT_FOUND_1, name);
        }
        return role;
    }

    /**
     * Create a session for the given user.
     *
     * @param user the user
     * @return the session
     * @throws DbException if the database is in exclusive mode
     */
    public ServerSession createSession(User user) {
        return createSession(user, null);
    }

    public synchronized ServerSession createSession(User user, ConnectionInfo ci) {
        if (exclusiveSession != null) {
            throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
        }
        ServerSession session = new ServerSession(this, user, ++nextSessionId);
        session.setConnectionInfo(ci);
        userSessions.add(session);
        session.getTrace().setType(TraceModuleType.DATABASE).info("connected session #{0} to {1}",
                session.getId(), name);
        if (delayedCloser != null) {
            delayedCloser.reset();
            delayedCloser = null;
        }
        return session;
    }

    /**
     * Remove a session. This method is called after the user has disconnected.
     *
     * @param session the session
     */
    public synchronized void removeSession(ServerSession session) {
        if (deleteFilesOnDisconnect) {
            userSessions.clear();
            drop();
            return;
        }
        if (session != null) {
            if (exclusiveSession == session) {
                setExclusiveSession(null, false);
            }
            userSessions.remove(session);
            if (session != systemSession && session.getTrace().isInfoEnabled()) {
                session.getTrace().setType(TraceModuleType.DATABASE).info("disconnected session #{0}",
                        session.getId());
            }
        }

        if (userSessions.isEmpty() && session != systemSession) {
            if (closeDelay == 0) {
                close(false);
            } else if (closeDelay < 0) {
                return;
            } else {
                delayedCloser = new DatabaseCloser(this, closeDelay * 1000, false);
                delayedCloser.setName(getShortName() + " database close delay");
                delayedCloser.setDaemon(true);
                delayedCloser.start();
            }
        }
    }

    private synchronized void closeAllSessionsException(ServerSession except) {
        ServerSession[] all = new ServerSession[userSessions.size()];
        userSessions.toArray(all);
        for (ServerSession s : all) {
            if (s != except) {
                try {
                    // must roll back, otherwise the session is removed and
                    // the transaction log that contains its uncommitted operations as well
                    s.rollback();
                    s.close();
                } catch (DbException e) {
                    trace.error(e, "disconnecting session #{0}", s.getId());
                }
            }
        }
    }

    /**
     * Close the database.
     *
     * @param fromShutdownHook true if this method is called from the shutdown
     *            hook
     */
    synchronized void close(boolean fromShutdownHook) {
        if (state == State.CLOSING || state == State.CLOSED) {
            return;
        }
        state = State.CLOSING;
        if (userSessions.size() > 0) {
            if (!fromShutdownHook) {
                return;
            }
            trace.info("closing {0} from shutdown hook", name);
            closeAllSessionsException(null);
        }
        trace.info("closing {0}", name);
        if (eventListener != null) {
            // allow the event listener to connect to the database
            state = State.OPENED;
            DatabaseEventListener e = eventListener;
            // set it to null, to make sure it's called only once
            eventListener = null;
            e.closingDatabase();
            if (userSessions.size() > 0) {
                // if a connection was opened, we can't close the database
                return;
            }
            state = State.CLOSING;
        }
        // remove all session variables
        if (persistent) {
            if (lobStorage != null) {
                boolean containsLargeObject = false;
                for (Schema schema : getAllSchemas()) {
                    for (Table table : schema.getAllTablesAndViews()) {
                        if (table.containsLargeObject()) {
                            containsLargeObject = true;
                            break;
                        }
                    }
                }
                // 避免在没有lob字段时在关闭数据库的最后反而去生成lob相关的文件
                if (containsLargeObject) {
                    try {
                        lobStorage.removeAllForTable(LobStorage.TABLE_ID_SESSION_VARIABLE);
                    } catch (DbException e) {
                        trace.error(e, "close");
                    }
                }
            }
        }
        try {
            if (systemSession != null) {
                for (Table table : getAllTablesAndViews(false)) {
                    if (table.isGlobalTemporary()) {
                        table.removeChildrenAndResources(systemSession, null);
                    } else {
                        table.close(systemSession);
                    }
                }
                for (SchemaObject obj : getAllSchemaObjects(DbObjectType.SEQUENCE)) {
                    Sequence sequence = (Sequence) obj;
                    sequence.close();
                }
                for (SchemaObject obj : getAllSchemaObjects(DbObjectType.TRIGGER)) {
                    TriggerObject trigger = (TriggerObject) obj;
                    try {
                        trigger.close();
                    } catch (SQLException e) {
                        trace.error(e, "close");
                    }
                }
                if (meta != null)
                    meta.close(systemSession);
                systemSession.commit();
            }
        } catch (DbException e) {
            trace.error(e, "close");
        }
        tempFileDeleter.deleteAll();
        try {
            closeOpenFilesAndUnlock(true);
        } catch (DbException e) {
            trace.error(e, "close");
        }
        trace.info("closed");
        traceSystem.close();
        if (closeOnExit != null) {
            closeOnExit.reset();
            try {
                ShutdownHookUtils.removeShutdownHook(closeOnExit);
            } catch (IllegalStateException e) {
                // ignore
            } catch (SecurityException e) {
                // applets may not do that - ignore
            }
            closeOnExit = null;
        }
        LealoneDatabase.getInstance().closeDatabase(name);

        for (Storage s : getStorages()) {
            s.close();
        }

        state = State.CLOSED;
    }

    /**
     * Close all open files and unlock the database.
     *
     * @param flush whether writing is allowed
     */
    private synchronized void closeOpenFilesAndUnlock(boolean flush) {
        if (persistent) {
            deleteOldTempFiles();
        }
        if (systemSession != null) {
            systemSession.close();
            systemSession = null;
        }
    }

    /**
     * Immediately close the database.
     */
    public void shutdownImmediately() {
        try {
            userSessions.clear();
            LealoneDatabase.getInstance().closeDatabase(name);
            for (Storage s : getStorages()) {
                s.closeImmediately();
            }
            if (traceSystem != null) {
                traceSystem.close();
            }
        } catch (DbException e) {
            DbException.traceThrowable(e);
        } finally {
            state = State.POWER_OFF;
        }
    }

    @Override
    public void checkPowerOff() {
        if (state == State.POWER_OFF)
            throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
    }

    public boolean isPowerOff() {
        return state == State.POWER_OFF;
    }

    @Override
    public void checkWritingAllowed() {
        if (readOnly) {
            throw DbException.get(ErrorCode.DATABASE_IS_READ_ONLY);
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public int getWriteDelay() {
        return dbSettings.writeDelay;
    }

    public ArrayList<Comment> getAllComments() {
        HashMap<String, Comment> map = getDbObjects(DbObjectType.COMMENT);
        return new ArrayList<>(map.values());
    }

    public ArrayList<Right> getAllRights() {
        HashMap<String, Right> map = getDbObjects(DbObjectType.RIGHT);
        return new ArrayList<>(map.values());
    }

    public ArrayList<Role> getAllRoles() {
        HashMap<String, Role> map = getDbObjects(DbObjectType.ROLE);
        return new ArrayList<>(map.values());
    }

    /**
     * Get all schema objects.
     *
     * @return all objects of all types
     */
    public ArrayList<SchemaObject> getAllSchemaObjects() {
        initInfoSchemaMetaTables();
        ArrayList<SchemaObject> list = new ArrayList<>();
        for (Schema schema : getAllSchemas()) {
            list.addAll(schema.getAll());
        }
        return list;
    }

    /**
     * Get all schema objects of the given type.
     *
     * @param type the object type
     * @return all objects of that type
     */
    public ArrayList<SchemaObject> getAllSchemaObjects(DbObjectType type) {
        if (type == DbObjectType.TABLE_OR_VIEW) {
            initInfoSchemaMetaTables();
        }
        ArrayList<SchemaObject> list = new ArrayList<>();
        for (Schema schema : getAllSchemas()) {
            list.addAll(schema.getAll(type));
        }
        return list;
    }

    /**
     * Get all tables and views.
     *
     * @param includeMeta whether to force including the meta data tables (if
     *            true, metadata tables are always included; if false, metadata
     *            tables are only included if they are already initialized)
     * @return all objects of that type
     */
    public ArrayList<Table> getAllTablesAndViews(boolean includeMeta) {
        if (includeMeta) {
            initInfoSchemaMetaTables();
        }
        ArrayList<Table> list = new ArrayList<>();
        for (Schema schema : getAllSchemas(includeMeta)) {
            list.addAll(schema.getAllTablesAndViews());
        }
        return list;
    }

    public ArrayList<Schema> getAllSchemas() {
        return getAllSchemas(true);
    }

    private ArrayList<Schema> getAllSchemas(boolean includeMeta) {
        if (includeMeta) {
            initInfoSchemaMetaTables();
        }
        HashMap<String, Schema> map = getDbObjects(DbObjectType.SCHEMA);
        return new ArrayList<>(map.values());
    }

    public ArrayList<User> getAllUsers() {
        HashMap<String, User> map = getDbObjects(DbObjectType.USER);
        return new ArrayList<>(map.values());
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    @Override
    public String getDatabasePath() {
        if (persistent) {
            return getStoragePath();
        }
        return null;
    }

    /**
     * Get all sessions that are currently connected to the database.
     *
     * @param includingSystemSession if the system session should also be
     *            included
     * @return the list of sessions
     */
    public ServerSession[] getSessions(boolean includingSystemSession) {
        ArrayList<ServerSession> list;
        // need to synchronized on userSession, otherwise the list
        // may contain null elements
        synchronized (userSessions) {
            list = new ArrayList<>(userSessions);
        }
        // copy, to ensure the reference is stable
        ServerSession sys = systemSession;
        if (includingSystemSession && sys != null) {
            list.add(sys);
        }
        ServerSession[] array = new ServerSession[list.size()];
        list.toArray(array);
        return array;
    }

    /**
     * Create a temporary file in the database folder.
     *
     * @return the file name
     */
    public String createTempFile() {
        try {
            boolean inTempDir = readOnly;
            String name = getStoragePath();
            if (!persistent) {
                name = "memFS:" + name;
            }
            return FileUtils.createTempFile(name, Constants.SUFFIX_TEMP_FILE, true, inTempDir);
        } catch (IOException e) {
            throw DbException.convertIOException(e, getStoragePath());
        }
    }

    private void deleteOldTempFiles() {
        String path = FileUtils.getParent(getStoragePath());
        for (String name : FileUtils.newDirectoryStream(path)) {
            if (name.endsWith(Constants.SUFFIX_TEMP_FILE) && name.startsWith(getStoragePath())) {
                // can't always delete the files, they may still be open
                FileUtils.tryDelete(name);
            }
        }
    }

    /**
     * Get the schema. If the schema does not exist, an exception is thrown.
     *
     * @param schemaName the name of the schema
     * @return the schema
     * @throws DbException no schema with that name exists
     */
    public Schema getSchema(ServerSession session, String schemaName) {
        Schema schema = findSchema(session, schemaName);
        if (schema == null) {
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        return schema;
    }

    /**
     * Get the first table that depends on this object.
     *
     * @param obj the object to find
     * @param except the table to exclude (or null)
     * @return the first dependent table, or null
     */
    public Table getDependentTable(SchemaObject obj, Table except) {
        switch (obj.getType()) {
        case COMMENT:
        case CONSTRAINT:
        case INDEX:
        case RIGHT:
        case TRIGGER:
        case USER:
            return null;
        default:
        }
        HashSet<DbObject> set = new HashSet<>();
        for (Table t : getAllTablesAndViews(false)) {
            if (except == t) {
                continue;
            }
            set.clear();
            t.addDependencies(set);
            if (set.contains(obj)) {
                return t;
            }
        }
        return null;
    }

    public void addPersistentMetaInfo(MetaTable mt, ArrayList<Row> rows) {
    }

    /**
     * Start collecting statistics.
     */
    public void statisticsStart() {
    }

    public HashMap<String, Integer> statisticsEnd() {
        return null;
    }

    public TraceSystem getTraceSystem() {
        return traceSystem;
    }

    public int getCacheSize() {
        return dbSettings.cacheSize;
    }

    public int getPageSize() {
        return dbSettings.pageSize;
    }

    public byte[] getFileEncryptionKey() {
        return dbSettings.fileEncryptionKey;
    }

    public Role getPublicRole() {
        return publicRole;
    }

    /**
     * Get a unique temporary table name.
     *
     * @param baseName the prefix of the returned name
     * @param session the session
     * @return a unique name
     */
    public synchronized String getTempTableName(String baseName, ServerSession session) {
        String tempName;
        do {
            tempName = baseName + "_COPY_" + session.getId() + "_" + nextTempTableId++;
        } while (mainSchema.findTableOrView(session, tempName) != null);
        return tempName;
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    public void setBackgroundException(DbException e) {
        if (backgroundException == null) {
            backgroundException = e;
            TraceSystem t = getTraceSystem();
            if (t != null) {
                t.getTrace(TraceModuleType.DATABASE).error(e, "flush");
            }
        }
    }

    /**
     * Flush all pending changes to the transaction log.
     */
    public synchronized void flush() {
        if (readOnly) {
            return;
        }
        try {
            for (Storage s : getStorages()) {
                s.save();
            }
        } catch (RuntimeException e) {
            backgroundException = DbException.convert(e);
            throw e;
        }
    }

    public void setEventListener(DatabaseEventListener eventListener) {
        this.eventListener = eventListener;
    }

    public void setEventListenerClass(String className) {
        if (className == null || className.length() == 0) {
            eventListener = null;
        } else {
            try {
                eventListener = (DatabaseEventListener) Utils.loadUserClass(className)
                        .getDeclaredConstructor().newInstance();
                eventListener.init(name);
            } catch (Throwable e) {
                throw DbException.get(ErrorCode.ERROR_SETTING_DATABASE_EVENT_LISTENER_2, e, className,
                        e.toString());
            }
        }
    }

    /**
     * Set the progress of a long running operation.
     * This method calls the {@link DatabaseEventListener} if one is registered.
     *
     * @param state the {@link DatabaseEventListener} state
     * @param name the object name
     * @param x the current position
     * @param max the highest value
     */
    public void setProgress(int state, String name, int x, int max) {
        if (eventListener != null) {
            try {
                eventListener.setProgress(state, name, x, max);
            } catch (Exception e2) {
                // ignore this (user made) exception
            }
        }
    }

    /**
     * This method is called after an exception occurred, to inform the database
     * event listener (if one is set).
     *
     * @param e the exception
     * @param sql the SQL statement
     */
    public void exceptionThrown(SQLException e, String sql) {
        if (eventListener != null) {
            try {
                eventListener.exceptionThrown(e, sql);
            } catch (Exception e2) {
                // ignore this (user made) exception
            }
        }
    }

    public int getAllowLiterals() {
        if (isStarting()) {
            return Constants.ALLOW_LITERALS_ALL;
        }
        return dbSettings.allowLiterals;
    }

    public int getMaxMemoryRows() {
        return dbSettings.maxMemoryRows;
    }

    public int getMaxMemoryUndo() {
        return dbSettings.maxMemoryUndo;
    }

    public int getMaxOperationMemory() {
        return dbSettings.maxOperationMemory;
    }

    public synchronized void setCloseDelay(int value) {
        this.closeDelay = value;
    }

    public SystemSession getSystemSession() {
        return systemSession;
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return persistent ? dbSettings.maxLengthInplaceLob : Integer.MAX_VALUE;
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return dbSettings.lobCompressionAlgorithm;
    }

    @Override
    public Object getLobSyncObject() {
        return lobSyncObject;
    }

    @Override
    public LobStorage getLobStorage() {
        return lobStorage;
    }

    public void setLobStorage(LobStorage lobStorage) {
        if (lobStorage != null) {
            this.lobStorage = lobStorage;
        }
    }

    public boolean getIgnoreCase() {
        if (isStarting()) {
            // tables created at startup must not be converted to ignorecase
            return false;
        }
        return dbSettings.ignoreCase;
    }

    public synchronized void setDeleteFilesOnDisconnect(boolean b) {
        this.deleteFilesOnDisconnect = b;
    }

    public boolean getDeleteFilesOnDisconnect() {
        return deleteFilesOnDisconnect;
    }

    public boolean getOptimizeReuseResults() {
        return dbSettings.optimizeReuseResults;
    }

    public boolean getReferentialIntegrity() {
        return dbSettings.referentialIntegrity;
    }

    public int getSessionCount() {
        return userSessions.size();
    }

    public void setQueryStatistics(boolean b) {
        synchronized (this) {
            if (!b) {
                queryStatisticsData = null;
            }
        }
    }

    public boolean getQueryStatistics() {
        return dbSettings.queryStatistics;
    }

    public void setQueryStatisticsMaxEntries(int n) {
        if (queryStatisticsData != null) {
            synchronized (this) {
                if (queryStatisticsData != null) {
                    queryStatisticsData.setMaxQueryEntries(n);
                }
            }
        }
    }

    public QueryStatisticsData getQueryStatisticsData() {
        if (!dbSettings.queryStatistics) {
            return null;
        }
        if (queryStatisticsData == null) {
            synchronized (this) {
                if (queryStatisticsData == null) {
                    queryStatisticsData = new QueryStatisticsData(dbSettings.queryStatisticsMaxEntries);
                }
            }
        }
        return queryStatisticsData;
    }

    /**
     * Check if the database is currently opening. This is true until all stored
     * SQL statements have been executed.
     *
     * @return true if the database is still starting
     */
    public boolean isStarting() {
        return state == State.STARTING;
    }

    private boolean isMetaReady() {
        return state != State.CONSTRUCTOR_CALLED && state != State.STARTING;
    }

    /**
     * Check if the database is in the process of closing.
     *
     * @return true if the database is closing
     */
    public boolean isClosing() {
        return state == State.CLOSING;
    }

    /**
     * Check if multi version concurrency is enabled for this database.
     *
     * @return true if it is enabled
     */
    public boolean isMultiVersion() {
        return transactionEngine.supportsMVCC();
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public Mode getMode() {
        return mode;
    }

    public ServerSession getExclusiveSession() {
        return exclusiveSession;
    }

    /**
     * Set the session that can exclusively access the database.
     *
     * @param session the session
     * @param closeOthers whether other sessions are closed
     */
    public synchronized void setExclusiveSession(ServerSession session, boolean closeOthers) {
        this.exclusiveSession = session;
        if (closeOthers) {
            closeAllSessionsException(session);
        }
        if (session == null && waitingSessions != null) {
            for (ServerSession s : waitingSessions) {
                s.setStatus(SessionStatus.TRANSACTION_NOT_COMMIT);
                s.getTransactionListener().wakeUp();
            }
            waitingSessions = null;
        }
    }

    public synchronized boolean addWaitingSession(ServerSession session) {
        if (exclusiveSession == null)
            return false;
        if (waitingSessions == null)
            waitingSessions = new LinkedList<>();
        waitingSessions.add(session);
        return true;
    }

    @Override
    public String toString() {
        return name + ":" + super.toString();
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return tempFileDeleter;
    }

    /**
     * Get the first user defined table.
     *
     * @return the table or null if no table is defined
     */
    public Table getFirstUserTable() {
        for (Table table : getAllTablesAndViews(false)) {
            if (DbObjectVersionManager.isDbObjectVersionTable(table.getName()))
                continue;
            if (table.getCreateSQL() != null) {
                if (table.isHidden()) {
                    // LOB tables
                    continue;
                }
                return table;
            }
        }
        return null;
    }

    /**
     * Flush all changes and open a new transaction log.
     */
    public void checkpoint() {
        if (persistent) {
            transactionEngine.checkpoint();
        }
        getTempFileDeleter().deleteUnused();
    }

    /**
     * Synchronize the files with the file system. This method is called when
     * executing the SQL statement CHECKPOINT SYNC.
     */
    public synchronized void sync() {
        if (readOnly) {
            return;
        }
        for (Storage s : getStorages()) {
            s.save();
        }
    }

    /**
     * Switch the database to read-only mode.
     *
     * @param readOnly the new value
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void setCompactMode(int compactMode) {
        this.compactMode = compactMode;
    }

    public int getCompactMode() {
        return compactMode;
    }

    public SourceCompiler getCompiler() {
        if (compiler == null) {
            compiler = new SourceCompiler();
        }
        return compiler;
    }

    public Connection getInternalConnection() {
        return systemSession.createConnection(systemUser.getName(), Constants.CONN_URL_INTERNAL);
    }

    public int getDefaultTableType() {
        return dbSettings.defaultTableType;
    }

    /**
     * Create a new hash map. Depending on the configuration, the key is case
     * sensitive or case insensitive.
     *
     * @param <V> the value type
     * @return the hash map
     */
    public <V> HashMap<String, V> newStringMap() {
        return dbSettings.databaseToUpper ? new HashMap<String, V>() : new CaseInsensitiveMap<V>();
    }

    /**
     * Compare two identifiers (table names, column names,...) and verify they
     * are equal. Case sensitivity depends on the configuration.
     *
     * @param a the first identifier
     * @param b the second identifier
     * @return true if they match
     */
    public boolean equalsIdentifiers(String a, String b) {
        if (a == b || a.equals(b)) {
            return true;
        }
        if (!dbSettings.databaseToUpper && a.equalsIgnoreCase(b)) {
            return true;
        }
        return false;
    }

    public void backupTo(String fileName) {
        for (Storage s : getStorages()) {
            s.backupTo(fileName);
        }
    }

    public Storage getMetaStorage() {
        return storages.get(metaStorageEngineName);
    }

    public List<Storage> getStorages() {
        return new ArrayList<>(storages.values());
    }

    public Storage getStorage(String storageName) {
        return storages.get(storageName);
    }

    public synchronized Storage getStorage(StorageEngine storageEngine) {
        Storage storage = storages.get(storageEngine.getName());
        if (storage != null)
            return storage;

        storage = getStorageBuilder(storageEngine).openStorage();
        storages.put(storageEngine.getName(), storage);
        if (persistent && lobStorage == null)
            setLobStorage(storageEngine.getLobStorage(this, storage));
        return storage;
    }

    public String getStoragePath() {
        if (storagePath != null)
            return storagePath;
        String baseDir = SysProperties.getBaseDir();
        baseDir = FileUtils.getDirWithSeparator(baseDir);

        String path;
        if (baseDir == null)
            path = "." + File.separator;
        else
            path = baseDir;

        path = path + "db" + Constants.NAME_SEPARATOR + id;
        try {
            path = new File(path).getCanonicalPath();
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        return path;
    }

    private StorageBuilder getStorageBuilder(StorageEngine storageEngine) {
        StorageBuilder storageBuilder = storageEngine.getStorageBuilder();
        if (!persistent) {
            storageBuilder.inMemory();
        } else {
            String storagePath = getStoragePath();
            byte[] key = getFileEncryptionKey();
            storageBuilder.pageSplitSize(getPageSize());
            storageBuilder.storagePath(storagePath);
            if (isReadOnly()) {
                storageBuilder.readOnly();
            }
            if (key != null) {
                char[] password = new char[key.length / 2];
                for (int i = 0; i < password.length; i++) {
                    password[i] = (char) (((key[i + i] & 255) << 16) | ((key[i + i + 1]) & 255));
                }
                storageBuilder.encryptionKey(password);
            }
            if (getSettings().compressData) {
                storageBuilder.compress();
                // use a larger page split size to improve the compression ratio
                int pageSize = getPageSize();
                int compressPageSize = 64 * 1024;
                if (pageSize > compressPageSize)
                    compressPageSize = pageSize;
                storageBuilder.pageSplitSize(compressPageSize);
            }
        }
        storageBuilder.backgroundExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                setBackgroundException(DbException.convert(e));
            }
        });
        storageBuilder.db(this);
        return storageBuilder;
    }

    @Override
    public String getSQL() {
        return quoteIdentifier(name);
    }

    private static String getCreateSQL(String quotedDbName, Map<String, String> parameters,
            Map<String, String> replicationProperties, Map<String, String> nodeAssignmentProperties,
            RunMode runMode) {
        StatementBuilder sql = new StatementBuilder("CREATE DATABASE IF NOT EXISTS ");
        sql.append(quotedDbName);
        if (runMode != null) {
            sql.append(" RUN MODE ").append(runMode.toString());
        }
        if (parameters != null && !parameters.isEmpty()) {
            sql.append(" PARAMETERS");
            appendMap(sql, parameters);
        }
        return sql.toString();
    }

    public static void appendMap(StatementBuilder sql, Map<String, String> map) {
        sql.resetCount();
        sql.append("(");
        for (Entry<String, String> e : map.entrySet()) {
            if (e.getValue() == null)
                continue;
            sql.appendExceptFirst(",");
            sql.append(e.getKey()).append('=').append("'").append(e.getValue()).append("'");
        }
        sql.append(')');
    }

    @Override
    public List<? extends DbObject> getChildren() {
        return getAllSchemaObjects();
    }

    @Override
    public Database getDatabase() {
        return this;
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQL(quoteIdentifier(name), parameters, replicationParameters,
                nodeAssignmentParameters, runMode);
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.DATABASE;
    }

    @Override
    public void removeChildrenAndResources(ServerSession session, DbObjectLock lock) {
    }

    @Override
    public void checkRename() {
    }

    @Override
    public void rename(String newName) {
    }

    @Override
    public boolean isTemporary() {
        return !persistent;
    }

    @Override
    public void setTemporary(boolean temporary) {
    }

    @Override
    public void setComment(String comment) {
    }

    @Override
    public String getComment() {
        return null;
    }

    public void createRootUserIfNotExists() {
        // 如果已经存在一个Admin权限的用户，那就不再创建root用户了
        // 最常见的是对默认的root用户重命名后会出现这种情况
        for (User user : getAllUsers()) {
            if (user.isAdmin())
                return;
        }
        // 新建session，避免使用system session
        try (ServerSession session = createSession(systemUser)) {
            // executeUpdate()会自动提交，所以不需要再调用一次commit
            session.prepareStatementLocal("CREATE USER IF NOT EXISTS root PASSWORD '' ADMIN")
                    .executeUpdate();
        }
    }

    synchronized User createAdminUser(String userName, byte[] userPasswordHash) {
        // 新建session，避免使用system session
        try (ServerSession session = createSession(systemUser)) {
            DbObjectLock lock = tryExclusiveAuthLock(session);
            User user = new User(this, allocateObjectId(), userName, false);
            user.setAdmin(true);
            user.setUserPasswordHash(userPasswordHash);
            addDatabaseObject(session, user, lock);
            session.commit();
            return user;
        }
    }

    public void drop() {
        if (getSessionCount() > 0) {
            setDeleteFilesOnDisconnect(true);
            return;
        }
        for (Storage storage : getStorages()) {
            storage.drop();
        }
    }

    public DbObjectVersionManager getVersionManager() {
        return dbObjectVersionManager;
    }
}
