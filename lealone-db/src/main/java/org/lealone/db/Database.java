/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.lealone.common.util.MathUtils;
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
import org.lealone.db.constraint.Constraint;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.SchemaObject;
import org.lealone.db.schema.Sequence;
import org.lealone.db.schema.TriggerObject;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.db.session.SystemSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.CreateTableData;
import org.lealone.db.table.IndexColumn;
import org.lealone.db.table.MetaTable;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableView;
import org.lealone.db.util.SourceCompiler;
import org.lealone.db.value.CompareMode;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.net.NetNode;
import org.lealone.net.NetNodeManager;
import org.lealone.net.NetNodeManagerHolder;
import org.lealone.sql.SQLEngine;
import org.lealone.sql.SQLEngineManager;
import org.lealone.sql.SQLParser;
import org.lealone.storage.LobStorage;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.memory.MemoryStorageEngine;
import org.lealone.storage.replication.ReplicationSession;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionEngineManager;

/**
 * There is one database object per open database.
 *
 * @author H2 Group
 * @author zhh
 */
public class Database implements DataHandler, DbObject, IDatabase {

    /**
     * The default name of the system user. This name is only used as long as
     * there is no administrator user registered.
     */
    private static final String SYSTEM_USER_NAME = "DBA";

    private final HashMap<String, User> users = new HashMap<>();
    private final HashMap<String, Role> roles = new HashMap<>();
    private final HashMap<String, Right> rights = new HashMap<>();
    private final HashMap<String, Setting> settings = new HashMap<>();
    private final HashMap<String, Comment> comments = new HashMap<>();
    private final HashMap<String, Schema> schemas = new HashMap<>();

    // 与users、roles和rights相关的操作都用这个对象进行同步
    private final Object authLock = new Object();

    public Object getAuthLock() {
        return authLock;
    }

    private final Set<ServerSession> userSessions = Collections.synchronizedSet(new HashSet<ServerSession>());
    private ServerSession exclusiveSession;
    private final BitField objectIds = new BitField();
    private final Object lobSyncObject = new Object();

    private Schema mainSchema;
    private Schema infoSchema;
    private int nextSessionId;
    private int nextTempTableId;
    private User systemUser;
    private SystemSession systemSession;
    private Table meta;
    private String metaStorageEngineName;
    private Index metaIdIndex;
    private boolean starting;
    private TraceSystem traceSystem;
    private Trace trace;
    private Role publicRole;
    private final AtomicLong modificationDataId = new AtomicLong();
    private final AtomicLong modificationMetaId = new AtomicLong();
    private CompareMode compareMode;
    private boolean readOnly;
    private int writeDelay = Constants.DEFAULT_WRITE_DELAY;
    private DatabaseEventListener eventListener;
    private int maxMemoryRows = Constants.DEFAULT_MAX_MEMORY_ROWS;
    private int maxMemoryUndo = Constants.DEFAULT_MAX_MEMORY_UNDO;
    private int lockMode = Constants.DEFAULT_LOCK_MODE;
    private int maxLengthInplaceLob;
    private int allowLiterals = Constants.ALLOW_LITERALS_ALL;

    private int powerOffCount;
    private int closeDelay = -1; // 不关闭
    private DatabaseCloser delayedCloser;
    private volatile boolean closing;
    private boolean ignoreCase;
    private boolean deleteFilesOnDisconnect;
    private String lobCompressionAlgorithm;
    private boolean optimizeReuseResults = true;
    private boolean referentialIntegrity = true;
    private final boolean multiVersion;
    private DatabaseCloser closeOnExit;
    private Mode mode = Mode.getDefaultMode();
    private int maxOperationMemory = Constants.DEFAULT_MAX_OPERATION_MEMORY;
    private final TempFileDeleter tempFileDeleter = TempFileDeleter.getInstance();
    private int cacheSize;
    private int compactMode;
    private SourceCompiler compiler;
    private volatile boolean metaTablesInitialized;
    private LobStorage lobStorage;
    private int defaultTableType = Table.TYPE_CACHED;
    private volatile boolean initialized = false;
    private DbException backgroundException;

    private boolean queryStatistics;
    private int queryStatisticsMaxEntries = Constants.QUERY_STATISTICS_MAX_ENTRIES;
    private QueryStatisticsData queryStatisticsData;

    private final int id;
    private final String name;
    private final Map<String, String> parameters;
    private final DbSettings dbSettings;
    private final boolean persistent;

    // 每个数据库只有一个SQL引擎和一个事务引擎
    private final SQLEngine sqlEngine;
    private final TransactionEngine transactionEngine;

    private String storagePath; // 不使用原始的名称，而是用id替换数据库名

    private Map<String, String> replicationParameters;
    private Map<String, String> nodeAssignmentParameters;

    private RunMode runMode = RunMode.CLIENT_SERVER;
    private ConnectionInfo lastConnectionInfo;

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
            this.parameters = new HashMap<>();
        }
        persistent = dbSettings.persistent;
        compareMode = CompareMode.getInstance(null, 0, false);
        if (dbSettings.mode != null) {
            mode = Mode.getInstance(dbSettings.mode);
        }
        maxLengthInplaceLob = SysProperties.LOB_IN_DATABASE ? Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB2
                : Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB;
        cacheSize = dbSettings.cacheSize;

        String engineName = dbSettings.defaultSQLEngine;
        SQLEngine sqlEngine = SQLEngineManager.getInstance().getEngine(engineName);
        if (sqlEngine == null) {
            try {
                sqlEngine = (SQLEngine) Utils.loadUserClass(engineName).newInstance();
                SQLEngineManager.getInstance().registerEngine(sqlEngine);
            } catch (Exception e) {
                e = new RuntimeException("Fatal error: the sql engine '" + engineName + "' not found", e);
                throw DbException.convert(e);
            }
        }
        this.sqlEngine = sqlEngine;

        engineName = dbSettings.defaultTransactionEngine;
        TransactionEngine transactionEngine = TransactionEngineManager.getInstance().getEngine(engineName);
        if (transactionEngine == null) {
            try {
                transactionEngine = (TransactionEngine) Utils.loadUserClass(engineName).newInstance();
                TransactionEngineManager.getInstance().registerEngine(transactionEngine);
            } catch (Exception e) {
                e = new RuntimeException("Fatal error: the transaction engine '" + engineName + "' not found", e);
                throw DbException.convert(e);
            }
        }
        this.transactionEngine = transactionEngine;
        multiVersion = transactionEngine.supportsMVCC();
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getShortName() {
        return getName();
    }

    @Override
    public String getSysMapName() {
        return "t_" + id + "_0"; // SYS(META)表对应存储层的MAP名称
    }

    public DbSettings getSettings() {
        return dbSettings;
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

    @Override
    public Map<String, String> getParameters() {
        return parameters;
    }

    public void alterParameters(Map<String, String> newParameters) {
        parameters.putAll(newParameters);
    }

    @Override
    public Map<String, String> getReplicationParameters() {
        return replicationParameters;
    }

    public void setReplicationParameters(Map<String, String> replicationParameters) {
        this.replicationParameters = replicationParameters;
    }

    @Override
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

    @Override
    public RunMode getRunMode() {
        return runMode;
    }

    @Override
    public boolean isShardingMode() {
        return runMode == RunMode.SHARDING;
    }

    @Override
    public synchronized Database copy() {
        Database db = new Database(id, name, parameters);
        // 因为每个存储只能打开一次，所以要复用原有存储
        db.storagePath = storagePath;
        db.storages.putAll(storages);
        db.runMode = runMode;
        db.replicationParameters = replicationParameters;
        db.replicationParameters = nodeAssignmentParameters;
        db.lastConnectionInfo = lastConnectionInfo;
        db.init();
        LealoneDatabase.getInstance().getDatabasesMap().put(name, db);
        for (ServerSession s : userSessions) {
            db.userSessions.add(s);
            s.setDatabase(db);
        }
        return db;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public synchronized void init() {
        if (initialized)
            return;
        initialized = true;

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
    }

    private boolean isLealoneDatabase() {
        return LealoneDatabase.ID == id;
    }

    private void initTraceSystem() {
        if (persistent) {
            traceSystem = new TraceSystem(getStoragePath() + Constants.SUFFIX_TRACE_FILE);
            traceSystem.setLevelFile(dbSettings.traceLevelFile);
            traceSystem.setLevelSystemOut(dbSettings.traceLevelSystemOut);
            trace = traceSystem.getTrace(TraceModuleType.DATABASE);
            trace.info("opening {0} (build {1})", name, Constants.BUILD_ID);
        } else {
            traceSystem = new TraceSystem();
            trace = traceSystem.getTrace(TraceModuleType.DATABASE);
        }
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

    private void openDatabase() {
        try {
            // 初始化traceSystem后才能做下面这些
            systemUser = new User(this, 0, SYSTEM_USER_NAME, true);
            systemUser.setAdmin(true);

            publicRole = new Role(this, 0, Constants.PUBLIC_ROLE_NAME, true);
            roles.put(Constants.PUBLIC_ROLE_NAME, publicRole);

            mainSchema = new Schema(this, 0, Constants.SCHEMA_MAIN, systemUser, true);
            infoSchema = new Schema(this, -1, "INFORMATION_SCHEMA", systemUser, true);
            schemas.put(mainSchema.getName(), mainSchema);
            schemas.put(infoSchema.getName(), infoSchema);

            systemSession = new SystemSession(this, systemUser, ++nextSessionId);

            openMetaTable();

            if (!readOnly) {
                // set CREATE_BUILD in a new database
                String name = SetType.CREATE_BUILD.getName();
                if (!settings.containsKey(name)) {
                    Setting setting = new Setting(this, allocateObjectId(), name);
                    setting.setIntValue(Constants.BUILD_ID);
                    lockMeta(systemSession);
                    addDatabaseObject(systemSession, setting);
                }
            }
            systemSession.commit();

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
        CreateTableData data = new CreateTableData();
        ArrayList<Column> cols = data.columns;
        Column columnId = new Column("ID", Value.INT);
        columnId.setNullable(false);
        cols.add(columnId);
        cols.add(new Column("TYPE", Value.INT));
        cols.add(new Column("SQL", Value.STRING));
        data.tableName = "SYS";
        data.id = 0;
        data.persistData = persistent;
        data.persistIndexes = persistent;
        data.create = true;
        data.isHidden = true;
        data.session = systemSession;
        data.storageEngineName = metaStorageEngineName = persistent ? getDefaultStorageEngineName()
                : MemoryStorageEngine.NAME;
        meta = mainSchema.createTable(data);

        IndexColumn[] pkCols = IndexColumn.wrap(new Column[] { columnId });
        IndexType indexType = IndexType.createDelegate(); // 重用原有的primary index
        metaIdIndex = meta.addIndex(systemSession, "SYS_ID", 0, pkCols, indexType, true, null);

        ArrayList<MetaRecord> records = new ArrayList<>();
        Cursor cursor = metaIdIndex.find(systemSession, null, null);
        while (cursor.next()) {
            MetaRecord rec = new MetaRecord(cursor.get());
            objectIds.set(rec.getId());
            records.add(rec);
        }

        objectIds.set(0);
        starting = true;

        Collections.sort(records);
        for (MetaRecord rec : records) {
            rec.execute(this, systemSession, eventListener);
        }

        recompileInvalidViews();
        starting = false;
    }

    public synchronized void rollbackMetaTable(ServerSession session) {
        ArrayList<MetaRecord> records = new ArrayList<>();
        Cursor cursor = metaIdIndex.find(systemSession, null, null);
        while (cursor.next()) {
            MetaRecord rec = new MetaRecord(cursor.get());
            objectIds.set(rec.getId());
            records.add(rec);
        }

        objectIds.set(0);
        starting = true;

        Collections.sort(records);
        for (MetaRecord rec : records) {
            rec.execute(this, systemSession, eventListener);
        }

        recompileInvalidViews();
        starting = false;
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

    public void setPowerOffCount(int count) {
        if (powerOffCount == -1) {
            return;
        }
        powerOffCount = count;
    }

    public int getPowerOffCount() {
        return powerOffCount;
    }

    @Override
    public void checkPowerOff() {
        if (powerOffCount == 0) {
            return;
        }
        if (powerOffCount > 1) {
            powerOffCount--;
            return;
        }
        if (powerOffCount != -1) {
            try {
                powerOffCount = -1;
                for (Storage s : getStorages()) {
                    s.closeImmediately();
                }
                if (traceSystem != null) {
                    traceSystem.close();
                }
            } catch (DbException e) {
                DbException.traceThrowable(e);
            }
        }
        LealoneDatabase.getInstance().closeDatabase(name);
        throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
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

    private void initMetaTables() {
        if (metaTablesInitialized) {
            return;
        }
        synchronized (infoSchema) {
            if (!metaTablesInitialized) {
                for (int type = 0, count = MetaTable.getMetaTableTypeCount(); type < count; type++) {
                    MetaTable m = new MetaTable(infoSchema, -1 - type, type);
                    infoSchema.add(m);
                }
                metaTablesInitialized = true;
            }
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

    /**
     * Verify the meta table is locked.
     *
     * @param session the session
     */
    public void verifyMetaLocked(ServerSession session) {
        if (!lockMeta(session) && lockMode != 0) {
            throw DbException.throwInternalError();
        }
    }

    /**
     * Lock the metadata table for updates.
     *
     * @param session the session
     * @return whether it was already locked before by this session
     */
    public boolean lockMeta(ServerSession session) {
        return false;
    }

    public void unlockMeta(ServerSession session) {
        meta.unlock(session);
    }

    private Cursor getMetaCursor(ServerSession session, int id) {
        SearchRow r = meta.getTemplateSimpleRow(false);
        r.setValue(0, ValueInt.get(id));
        return metaIdIndex.find(session, r, r);
    }

    // 用于测试
    public SearchRow findMeta(ServerSession session, int id) {
        Cursor cursor = getMetaCursor(session, id);
        cursor.next();
        return cursor.getSearchRow();
    }

    @SuppressWarnings("unchecked")
    private Map<String, DbObject> getMap(DbObjectType type) {
        Map<String, ? extends DbObject> result;
        switch (type) {
        case USER:
            result = users;
            break;
        case SETTING:
            result = settings;
            break;
        case ROLE:
            result = roles;
            break;
        case RIGHT:
            result = rights;
            break;
        case SCHEMA:
            result = schemas;
            break;
        case COMMENT:
            result = comments;
            break;
        case DATABASE:
            result = LealoneDatabase.getInstance().getDatabasesMap();
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        return (Map<String, DbObject>) result;
    }

    public Object getLock(DbObjectType type) {
        Object lock;
        switch (type) {
        case USER:
            lock = authLock;
            break;
        case SETTING:
            lock = settings;
            break;
        case ROLE:
            lock = authLock;
            break;
        case RIGHT:
            lock = rights;
            break;
        case SCHEMA:
            lock = schemas;
            break;
        case COMMENT:
            lock = comments;
            break;
        case DATABASE:
            lock = LealoneDatabase.getInstance().getDatabasesMap();
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        return lock;
    }

    /**
     * Add a schema object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public void addSchemaObject(ServerSession session, SchemaObject obj) {
        int id = obj.getId();
        if (id > 0 && !starting) {
            checkWritingAllowed();
        }
        // lockMeta(session);
        obj.getSchema().add(obj);
        addMeta(session, obj);
    }

    private void addMeta(ServerSession session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && !starting && !obj.isTemporary()) {
            Row r = meta.getTemplateRow();
            MetaRecord rec = new MetaRecord(obj);
            rec.setRecord(r);
            synchronized (objectIds) {
                objectIds.set(id);
            }
            // if (SysProperties.CHECK) {
            // verifyMetaLocked(session);
            // }
            meta.addRow(session, r);
        }
    }

    private void updateMetaAndFirstLevelChildren(ServerSession session, DbObject obj) {
        List<DbObject> list = obj.getChildren();
        Comment comment = findComment(obj);
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
     * Remove the given object from the meta data.
     *
     * @param session the session
     * @param id the id of the object to remove
     */
    public void removeMeta(ServerSession session, int id) {
        if (id > 0 && !starting) {
            SearchRow r = meta.getTemplateSimpleRow(false);
            r.setValue(0, ValueInt.get(id));
            Cursor cursor = metaIdIndex.find(session, r, r);
            if (cursor.next()) {
                Row found = cursor.get();
                meta.removeRow(session, found);
                synchronized (objectIds) {
                    objectIds.clear(id);
                }
            }
        }
    }

    /**
     * Update an object in the system table.
     *
     * @param session the session
     * @param obj the database object
     */
    public void updateMeta(ServerSession session, DbObject obj) {
        // lockMeta(session);
        int id = obj.getId();
        removeMeta(session, id);
        addMeta(session, obj);
    }

    /**
     * Add an object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public void addDatabaseObject(ServerSession session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && !starting) {
            checkWritingAllowed();
        }
        DbObjectType type = obj.getType();
        synchronized (getLock(type)) {
            Map<String, DbObject> map = getMap(type);
            String name = obj.getName();
            if (SysProperties.CHECK && map.get(name) != null) {
                DbException.throwInternalError("object already exists");
            }
            // lockMeta(session);
            addMeta(session, obj);
            map.put(name, obj);
        }
    }

    /**
     * Get the comment for the given database object if one exists, or null if
     * not.
     *
     * @param object the database object
     * @return the comment or null
     */
    public Comment findComment(DbObject object) {
        if (object.getType() == DbObjectType.COMMENT) {
            return null;
        }
        String key = Comment.getKey(object);
        return comments.get(key);
    }

    /**
     * Get the role if it exists, or null if not.
     *
     * @param roleName the name of the role
     * @return the role or null
     */
    public Role findRole(String roleName) {
        synchronized (getAuthLock()) {
            return roles.get(roleName);
        }
    }

    /**
     * Get the schema if it exists, or null if not.
     *
     * @param schemaName the name of the schema
     * @return the schema or null
     */
    public Schema findSchema(String schemaName) {
        Schema schema = schemas.get(schemaName);
        if (schema == infoSchema) {
            initMetaTables();
        }
        return schema;
    }

    /**
     * Get the setting if it exists, or null if not.
     *
     * @param name the name of the setting
     * @return the setting or null
     */
    public Setting findSetting(String name) {
        return settings.get(name);
    }

    /**
     * Get the user if it exists, or null if not.
     *
     * @param name the name of the user
     * @return the user or null
     */
    public User findUser(String name) {
        synchronized (getAuthLock()) {
            return users.get(name);
        }
    }

    /**
     * Get user with the given name. This method throws an exception if the user
     * does not exist.
     *
     * @param name the user name
     * @return the user
     * @throws DbException if the user does not exist
     */
    public User getUser(String name) {
        User user = findUser(name);
        if (user == null) {
            throw DbException.get(ErrorCode.USER_NOT_FOUND_1, name);
        }
        return user;
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
        session.getTrace().setType(TraceModuleType.DATABASE).info("connected session #{0} to {1}", session.getId(),
                name);
        if (delayedCloser != null) {
            delayedCloser.reset();
            delayedCloser = null;
        }
        return session;
    }

    @Override
    public ServerSession createInternalSession() {
        // User admin = null;
        // for (User user : getAllUsers()) {
        // if (user.isAdmin()) {
        // admin = user;
        // break;
        // }
        // }
        // if (admin == null) {
        // DbException.throwInternalError("no admin");
        // }
        if (lastConnectionInfo == null)
            throw DbException.throwInternalError("lastConnectionInfo is null");
        User user = getUser(lastConnectionInfo.getUserName());
        ServerSession session = createSession(user);
        session.setConnectionInfo(lastConnectionInfo);
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
                exclusiveSession = null;
            }
            userSessions.remove(session);
            if (session != systemSession && session.getTrace().isInfoEnabled()) {
                session.getTrace().setType(TraceModuleType.DATABASE).info("disconnected session #{0}", session.getId());
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
        if (closing) {
            return;
        }

        closing = true;
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
            closing = false;
            DatabaseEventListener e = eventListener;
            // set it to null, to make sure it's called only once
            eventListener = null;
            e.closingDatabase();
            if (userSessions.size() > 0) {
                // if a connection was opened, we can't close the database
                return;
            }
            closing = true;
        }
        // remove all session variables
        if (persistent) {
            if (lobStorage != null) {
                boolean containsLargeObject = false;
                for (Schema schema : schemas.values()) {
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
                if (powerOffCount != -1) {
                    for (Table table : getAllTablesAndViews(false)) {
                        if (table.isGlobalTemporary()) {
                            table.removeChildrenAndResources(systemSession);
                        } else {
                            table.close(systemSession);
                        }
                    }
                    for (SchemaObject obj : getAllSchemaObjects(DbObjectType.SEQUENCE)) {
                        Sequence sequence = (Sequence) obj;
                        sequence.close();
                    }
                }
                for (SchemaObject obj : getAllSchemaObjects(DbObjectType.TRIGGER)) {
                    TriggerObject trigger = (TriggerObject) obj;
                    try {
                        trigger.close();
                    } catch (SQLException e) {
                        trace.error(e, "close");
                    }
                }
                if (powerOffCount != -1) {
                    if (meta != null)
                        meta.close(systemSession);
                    systemSession.commit();
                }
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

    public ArrayList<Comment> getAllComments() {
        return new ArrayList<>(comments.values());
    }

    public int getAllowLiterals() {
        if (starting) {
            return Constants.ALLOW_LITERALS_ALL;
        }
        return allowLiterals;
    }

    public ArrayList<Right> getAllRights() {
        return new ArrayList<>(rights.values());
    }

    public ArrayList<Role> getAllRoles() {
        synchronized (getAuthLock()) {
            return new ArrayList<>(roles.values());
        }
    }

    /**
     * Get all schema objects.
     *
     * @return all objects of all types
     */
    public ArrayList<SchemaObject> getAllSchemaObjects() {
        initMetaTables();
        ArrayList<SchemaObject> list = new ArrayList<>();
        for (Schema schema : schemas.values()) {
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
            initMetaTables();
        }
        ArrayList<SchemaObject> list = new ArrayList<>();
        for (Schema schema : schemas.values()) {
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
            initMetaTables();
        }
        ArrayList<Table> list = new ArrayList<>();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAllTablesAndViews());
        }
        return list;
    }

    public ArrayList<Schema> getAllSchemas() {
        initMetaTables();
        return new ArrayList<>(schemas.values());
    }

    public ArrayList<Setting> getAllSettings() {
        return new ArrayList<>(settings.values());
    }

    public ArrayList<User> getAllUsers() {
        synchronized (getAuthLock()) {
            return new ArrayList<>(users.values());
        }
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
     * Rename a schema object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public void renameSchemaObject(ServerSession session, SchemaObject obj, String newName) {
        checkWritingAllowed();
        obj.getSchema().rename(obj, newName);
        updateMetaAndFirstLevelChildren(session, obj);
    }

    /**
     * Rename a database object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public void renameDatabaseObject(ServerSession session, DbObject obj, String newName) {
        checkWritingAllowed();
        DbObjectType type = obj.getType();
        synchronized (getLock(type)) {
            Map<String, DbObject> map = getMap(type);
            String oldName = obj.getName();
            if (SysProperties.CHECK) {
                if (!map.containsKey(oldName)) {
                    DbException.throwInternalError("not found: " + oldName);
                }
                if (oldName.equals(newName) || map.containsKey(newName)) {
                    DbException.throwInternalError("object already exists: " + newName);
                }
            }
            obj.checkRename();
            int id = obj.getId();
            // lockMeta(session);
            removeMeta(session, id);
            map.remove(oldName);
            obj.rename(newName);
            map.put(newName, obj);
        }
        updateMetaAndFirstLevelChildren(session, obj);
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
    public Schema getSchema(String schemaName) {
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        return schema;
    }

    /**
     * Remove the object from the database.
     *
     * @param session the session
     * @param obj the object to remove
     */
    public void removeDatabaseObject(ServerSession session, DbObject obj) {
        checkWritingAllowed();
        String objName = obj.getName();
        DbObjectType type = obj.getType();
        synchronized (getLock(type)) {
            Map<String, DbObject> map = getMap(type);
            if (SysProperties.CHECK && !map.containsKey(objName)) {
                DbException.throwInternalError("not found: " + objName);
            }
            Comment comment = findComment(obj);
            // lockMeta(session);
            if (comment != null) {
                removeDatabaseObject(session, comment);
            }
            int id = obj.getId();
            obj.removeChildrenAndResources(session);
            map.remove(objName);
            removeMeta(session, id);
        }
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

    /**
     * Remove an object from the system table.
     *
     * @param session the session
     * @param obj the object to be removed
     */
    public synchronized void removeSchemaObject(ServerSession session, SchemaObject obj) {
        DbObjectType type = obj.getType();
        if (type == DbObjectType.TABLE_OR_VIEW) {
            Table table = (Table) obj;
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTable(table);
                return;
            }
        } else if (type == DbObjectType.INDEX) {
            Index index = (Index) obj;
            Table table = index.getTable();
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTableIndex(index);
                return;
            }
        } else if (type == DbObjectType.CONSTRAINT) {
            Constraint constraint = (Constraint) obj;
            Table table = constraint.getTable();
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTableConstraint(constraint);
                return;
            }
        }
        checkWritingAllowed();
        lockMeta(session);
        Comment comment = findComment(obj);
        if (comment != null) {
            removeDatabaseObject(session, comment);
        }
        obj.getSchema().remove(obj);
        if (!starting) {
            Table t = getDependentTable(obj, null);
            if (t != null) {
                obj.getSchema().add(obj);
                throw DbException.get(ErrorCode.CANNOT_DROP_2, obj.getSQL(), t.getSQL());
            }
            obj.removeChildrenAndResources(session);
        }
        removeMeta(session, obj.getId());
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

    // TODO 传递到存储引擎
    public synchronized void setCacheSize(int kb) {
        if (starting) {
            int max = MathUtils.convertLongToInt(Utils.getMemoryMax()) / 2;
            kb = Math.min(kb, max);
        }
        cacheSize = kb;
    }

    public int getCacheSize() {
        return cacheSize;
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

    @Override
    public void checkWritingAllowed() {
        if (readOnly) {
            throw DbException.get(ErrorCode.DATABASE_IS_READ_ONLY);
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setWriteDelay(int value) {
        writeDelay = value;
    }

    public int getWriteDelay() {
        return writeDelay;
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
                eventListener = (DatabaseEventListener) Utils.loadUserClass(className).newInstance();
                eventListener.init(name);
            } catch (Throwable e) {
                throw DbException.get(ErrorCode.ERROR_SETTING_DATABASE_EVENT_LISTENER_2, e, className, e.toString());
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

    public int getMaxMemoryRows() {
        return maxMemoryRows;
    }

    public void setMaxMemoryRows(int value) {
        this.maxMemoryRows = value;
    }

    public void setMaxMemoryUndo(int value) {
        this.maxMemoryUndo = value;
    }

    public int getMaxMemoryUndo() {
        return maxMemoryUndo;
    }

    public void setLockMode(int lockMode) {
        switch (lockMode) {
        case Constants.LOCK_MODE_OFF:
        case Constants.LOCK_MODE_READ_COMMITTED:
        case Constants.LOCK_MODE_TABLE:
        case Constants.LOCK_MODE_TABLE_GC:
            break;
        default:
            throw DbException.getInvalidValueException("lock mode", lockMode);
        }
        this.lockMode = lockMode;
    }

    public int getLockMode() {
        return lockMode;
    }

    public synchronized void setCloseDelay(int value) {
        this.closeDelay = value;
    }

    public SystemSession getSystemSession() {
        return systemSession;
    }

    /**
     * Check if the database is in the process of closing.
     *
     * @return true if the database is closing
     */
    public boolean isClosing() {
        return closing;
    }

    public void setMaxLengthInplaceLob(int value) {
        this.maxLengthInplaceLob = value;
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return persistent ? maxLengthInplaceLob : Integer.MAX_VALUE;
    }

    public void setIgnoreCase(boolean b) {
        ignoreCase = b;
    }

    public boolean getIgnoreCase() {
        if (starting) {
            // tables created at startup must not be converted to ignorecase
            return false;
        }
        return ignoreCase;
    }

    public synchronized void setDeleteFilesOnDisconnect(boolean b) {
        this.deleteFilesOnDisconnect = b;
    }

    public boolean getDeleteFilesOnDisconnect() {
        return deleteFilesOnDisconnect;
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return lobCompressionAlgorithm;
    }

    public void setLobCompressionAlgorithm(String stringValue) {
        this.lobCompressionAlgorithm = stringValue;
    }

    public void setMaxLogSize(long value) {
    }

    public void setAllowLiterals(int value) {
        this.allowLiterals = value;
    }

    public boolean getOptimizeReuseResults() {
        return optimizeReuseResults;
    }

    public void setOptimizeReuseResults(boolean b) {
        optimizeReuseResults = b;
    }

    @Override
    public Object getLobSyncObject() {
        return lobSyncObject;
    }

    public int getSessionCount() {
        return userSessions.size();
    }

    public void setReferentialIntegrity(boolean b) {
        referentialIntegrity = b;
    }

    public boolean getReferentialIntegrity() {
        return referentialIntegrity;
    }

    public void setQueryStatistics(boolean b) {
        queryStatistics = b;
        synchronized (this) {
            if (!b) {
                queryStatisticsData = null;
            }
        }
    }

    public boolean getQueryStatistics() {
        return queryStatistics;
    }

    public void setQueryStatisticsMaxEntries(int n) {
        queryStatisticsMaxEntries = n;
        if (queryStatisticsData != null) {
            synchronized (this) {
                if (queryStatisticsData != null) {
                    queryStatisticsData.setMaxQueryEntries(queryStatisticsMaxEntries);
                }
            }
        }
    }

    public QueryStatisticsData getQueryStatisticsData() {
        if (!queryStatistics) {
            return null;
        }
        if (queryStatisticsData == null) {
            synchronized (this) {
                if (queryStatisticsData == null) {
                    queryStatisticsData = new QueryStatisticsData(queryStatisticsMaxEntries);
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
    @Override
    public boolean isStarting() {
        return starting;
    }

    /**
     * Check if multi version concurrency is enabled for this database.
     *
     * @return true if it is enabled
     */
    public boolean isMultiVersion() {
        return multiVersion;
    }

    /**
     * Called after the database has been opened and initialized. This method
     * notifies the event listener if one has been set.
     */
    void opened() {
        if (eventListener != null) {
            eventListener.opened();
        }
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMaxOperationMemory(int maxOperationMemory) {
        this.maxOperationMemory = maxOperationMemory;
    }

    public int getMaxOperationMemory() {
        return maxOperationMemory;
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
    public void setExclusiveSession(ServerSession session, boolean closeOthers) {
        this.exclusiveSession = session;
        if (closeOthers) {
            closeAllSessionsException(session);
        }
    }

    @Override
    public String toString() {
        return name + ":" + super.toString();
    }

    /**
     * Immediately close the database.
     */
    public void shutdownImmediately() {
        setPowerOffCount(1);
        try {
            checkPowerOff();
        } catch (DbException e) {
            // ignore
        }
        for (Storage s : getStorages()) {
            s.closeImmediately();
        }
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

    @Override
    public LobStorage getLobStorage() {
        return lobStorage;
    }

    public void setLobStorage(LobStorage lobStorage) {
        if (lobStorage != null) {
            this.lobStorage = lobStorage;
        }
    }

    public Connection getInternalConnection() {
        return systemSession.createConnection(systemUser.getName(), Constants.CONN_URL_INTERNAL);
    }

    public int getDefaultTableType() {
        return defaultTableType;
    }

    public void setDefaultTableType(int defaultTableType) {
        this.defaultTableType = defaultTableType;
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

    @Override
    public int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length) {
        throw DbException.throwInternalError();
    }

    public void backupTo(String fileName) {
        for (Storage s : getStorages()) {
            s.backupTo(fileName);
        }
    }

    private final ConcurrentHashMap<String, Storage> storages = new ConcurrentHashMap<>();

    public Storage getMetaStorage() {
        return storages.get(metaStorageEngineName);
    }

    @Override
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

    private String getStoragePath() {
        if (storagePath != null)
            return storagePath;
        String baseDir = SysProperties.getBaseDir();
        if (baseDir != null && !baseDir.endsWith(File.separator))
            baseDir = baseDir + File.separator;

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
        storagePath = path;
        return storagePath;
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
            Map<String, String> replicationProperties, Map<String, String> nodeAssignmentProperties, RunMode runMode) {
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
    public List<DbObject> getChildren() {
        return null;
    }

    @Override
    public Database getDatabase() {
        return this;
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQL(quoteIdentifier(name), parameters, replicationParameters, nodeAssignmentParameters,
                runMode);
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
    public void removeChildrenAndResources(ServerSession session) {
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

    private String[] hostIds;
    private HashSet<NetNode> nodes;
    private String targetNodes;

    @Override
    public String[] getHostIds() {
        if (hostIds == null) {
            synchronized (this) {
                if (hostIds == null) {
                    if (parameters != null && parameters.containsKey("hostIds")) {
                        targetNodes = parameters.get("hostIds").trim();
                        hostIds = StringUtils.arraySplit(targetNodes, ',');
                    }
                    if (hostIds == null) {
                        hostIds = new String[0];
                        nodes = null;
                    } else {
                        nodes = new HashSet<>(hostIds.length);
                        for (String id : hostIds) {
                            nodes.add(NetNode.createTCP(id));
                        }
                    }
                    if (nodes != null && nodes.isEmpty()) {
                        nodes = null;
                    }
                    if (targetNodes != null && targetNodes.isEmpty())
                        targetNodes = null;
                }
            }
        }
        return hostIds;
    }

    @Override
    public void setHostIds(String[] hostIds) {
        this.hostIds = null;
        if (hostIds != null && hostIds.length > 0)
            parameters.put("hostIds", StringUtils.arrayCombine(hostIds, ','));
        else
            parameters.put("hostIds", "");
        getHostIds();
    }

    public boolean isTargetNode(NetNode node) {
        if (hostIds == null) {
            getHostIds();
        }
        return nodes == null || nodes.contains(node);
    }

    public String getTargetNodes() {
        if (hostIds == null) {
            getHostIds();
        }
        return targetNodes;
    }

    public void createRootUserIfNotExists() {
        // 如果已经存在一个Admin权限的用户，那就不再创建root用户了
        // 最常见的是对默认的root用户重命名后会出现这种情况
        for (User user : getAllUsers()) {
            if (user.isAdmin())
                return;
        }
        ServerSession session = getSystemSession();
        // executeUpdate()会自动提交，所以不需要再调用一次commit
        session.prepareStatementLocal("CREATE USER IF NOT EXISTS root PASSWORD '' ADMIN").executeUpdate();
    }

    synchronized User createAdminUser(String userName, byte[] userPasswordHash) {
        User user = new User(this, allocateObjectId(), userName, false);
        user.setAdmin(true);
        user.setUserPasswordHash(userPasswordHash);
        lockMeta(systemSession);
        addDatabaseObject(systemSession, user);
        systemSession.commit();
        return user;
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

    public void setLastConnectionInfo(ConnectionInfo ci) {
        lastConnectionInfo = ci;
    }

    @Override
    public void notifyRunModeChanged() {
        String hostIds = getParameters().get("hostIds");
        for (ServerSession session : getSessions(false)) {
            session.runModeChanged(hostIds);
        }
    }

    @Override
    public Session createInternalSession(boolean useSystemDatabase) {
        return LealoneDatabase.getInstance().createInternalSession();
    }

    @Override
    public ReplicationSession createReplicationSession(Session session, Collection<NetNode> replicationNodes) {
        return getNetNodeManager().createReplicationSession(session, replicationNodes);
    }

    @Override
    public ReplicationSession createReplicationSession(Session session, Collection<NetNode> replicationNodes,
            Boolean remote) {
        return getNetNodeManager().createReplicationSession(session, replicationNodes, remote);
    }

    @Override
    public NetNode getNode(String hostId) {
        return getNetNodeManager().getNode(hostId);
    }

    @Override
    public String getHostId(NetNode node) {
        return getNetNodeManager().getHostId(node);
    }

    @Override
    public String getLocalHostId() {
        return NetNode.getLocalTcpHostAndPort();
    }

    @Override
    public List<NetNode> getReplicationNodes(Set<NetNode> oldReplicationNodes, Set<NetNode> candidateNodes) {
        return getNetNodeManager().getReplicationNodes(this, oldReplicationNodes, candidateNodes);
    }

    public DbObjectVersionManager getVersionManager() {
        return dbObjectVersionManager;
    }

    private static NetNodeManager getNetNodeManager() {
        return NetNodeManagerHolder.get();
    }
}
