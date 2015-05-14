/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.engine;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.api.DatabaseEventListener;
import org.lealone.api.ErrorCode;
import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.Comment;
import org.lealone.dbobject.DbObject;
import org.lealone.dbobject.Right;
import org.lealone.dbobject.Role;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.SchemaObject;
import org.lealone.dbobject.Sequence;
import org.lealone.dbobject.Setting;
import org.lealone.dbobject.TriggerObject;
import org.lealone.dbobject.User;
import org.lealone.dbobject.UserAggregate;
import org.lealone.dbobject.UserDataType;
import org.lealone.dbobject.constraint.Constraint;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.Index;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.MetaTable;
import org.lealone.dbobject.table.Table;
import org.lealone.dbobject.table.TableBase;
import org.lealone.dbobject.table.TableView;
import org.lealone.fs.FileUtils;
import org.lealone.jdbc.JdbcConnection;
import org.lealone.message.DbException;
import org.lealone.message.Trace;
import org.lealone.message.TraceSystem;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.storage.FileStore;
import org.lealone.storage.LobStorage;
import org.lealone.storage.StorageEngine;
import org.lealone.transaction.TransactionEngine;
import org.lealone.util.BitField;
import org.lealone.util.MathUtils;
import org.lealone.util.New;
import org.lealone.util.SmallLRUCache;
import org.lealone.util.SourceCompiler;
import org.lealone.util.StringUtils;
import org.lealone.util.TempFileDeleter;
import org.lealone.util.Utils;
import org.lealone.value.CaseInsensitiveMap;
import org.lealone.value.CompareMode;
import org.lealone.value.Value;
import org.lealone.value.ValueInt;

/**
 * There is one database object per open database.
 *
 * The format of the meta data table is:
 *  id int, 0, objectType int, sql varchar
 *
 * @since 2004-04-15 22:49
 */
public class Database implements DataHandler {
    /**
     * This log mode means the transaction log is not used.
     */
    public static final int LOG_MODE_OFF = 0;

    /**
     * This log mode means the transaction log is used and FileDescriptor.sync()
     * is called for each checkpoint. This is the default level.
     */
    public static final int LOG_MODE_SYNC = 2;

    private static int initialPowerOffCount;

    /**
     * The default name of the system user. This name is only used as long as
     * there is no administrator user registered.
     */
    private static final String SYSTEM_USER_NAME = "DBA";

    private final DatabaseEngine dbEngine;
    private final boolean persistent;
    private String databaseName;
    private String databaseShortName;
    private String databaseURL;
    private String cipher;
    private byte[] filePasswordHash;
    private byte[] fileEncryptionKey;

    private final HashMap<String, Role> roles = New.hashMap();
    private final HashMap<String, User> users = New.hashMap();
    private final HashMap<String, Setting> settings = New.hashMap();
    private final HashMap<String, Schema> schemas = New.hashMap();
    private final HashMap<String, Right> rights = New.hashMap();
    private final HashMap<String, UserDataType> userDataTypes = New.hashMap();
    private final HashMap<String, UserAggregate> aggregates = New.hashMap();
    private final HashMap<String, Comment> comments = New.hashMap();

    private final Set<Session> userSessions = Collections.synchronizedSet(new HashSet<Session>());
    private Session exclusiveSession;
    private final BitField objectIds = new BitField();
    private final Object lobSyncObject = new Object();

    private Schema mainSchema;
    private Schema infoSchema;
    private int nextSessionId;
    private int nextTempTableId;
    private User systemUser;
    private Session systemSession;
    private Table meta;
    private Index metaIdIndex;
    private boolean starting;
    private TraceSystem traceSystem;
    private Trace trace;
    private Role publicRole;
    private long modificationDataId;
    private long modificationMetaId;
    private CompareMode compareMode;
    private boolean readOnly;
    private int writeDelay = Constants.DEFAULT_WRITE_DELAY;
    private DatabaseEventListener eventListener;
    private int maxMemoryRows = Constants.DEFAULT_MAX_MEMORY_ROWS;
    private int maxMemoryUndo = Constants.DEFAULT_MAX_MEMORY_UNDO;
    private int lockMode = Constants.DEFAULT_LOCK_MODE;
    private int maxLengthInplaceLob;
    private int allowLiterals = Constants.ALLOW_LITERALS_ALL;

    private int powerOffCount = initialPowerOffCount;
    private int closeDelay = -1; //不关闭
    private DatabaseCloser delayedCloser;
    private volatile boolean closing;
    private boolean ignoreCase;
    private boolean deleteFilesOnDisconnect;
    private String lobCompressionAlgorithm;
    private boolean optimizeReuseResults = true;
    private boolean referentialIntegrity = true;
    private boolean multiVersion;
    private DatabaseCloser closeOnExit;
    private Mode mode = Mode.getInstance(Mode.REGULAR);
    private boolean multiThreaded = true; //如果是false，整个数据库是串行的
    private int maxOperationMemory = Constants.DEFAULT_MAX_OPERATION_MEMORY;
    private SmallLRUCache<String, String[]> lobFileListCache;
    private final TempFileDeleter tempFileDeleter = TempFileDeleter.getInstance();
    private volatile int checkpointAllowed;
    private int cacheSize;
    private int compactMode;
    private SourceCompiler compiler;
    private volatile boolean metaTablesInitialized;
    private LobStorage lobStorage;
    private int pageSize;
    private int defaultTableType = Table.TYPE_CACHED;
    private DbSettings dbSettings;
    private int logMode;
    private boolean initialized = false;

    public Database(DatabaseEngine dbEngine, boolean persistent) {
        this.dbEngine = dbEngine;
        this.persistent = persistent;
    }

    public DatabaseEngine getDatabaseEngine() {
        return dbEngine;
    }

    public String getStorageEngineName() {
        return dbSettings.defaultStorageEngine;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public synchronized void init(ConnectionInfo ci, String databaseShortName, String cipher) {
        if (initialized)
            return;
        this.dbSettings = ci.getDbSettings();
        this.compareMode = CompareMode.getInstance(null, 0, false);
        this.filePasswordHash = ci.getFilePasswordHash();
        this.fileEncryptionKey = ci.getFileEncryptionKey();
        this.databaseName = ci.getDatabaseName();
        this.databaseShortName = databaseShortName;
        this.maxLengthInplaceLob = SysProperties.LOB_IN_DATABASE ? Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB2
                : Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB;
        this.cipher = cipher;
        this.cacheSize = ci.getProperty("CACHE_SIZE", Constants.DEFAULT_CACHE_SIZE);
        this.pageSize = ci.getProperty("PAGE_SIZE", Constants.DEFAULT_PAGE_SIZE);
        this.databaseURL = ci.getURL();
        String listener = ci.removeProperty("DATABASE_EVENT_LISTENER", null);
        if (listener != null) {
            listener = StringUtils.trim(listener, true, true, "'");
            setEventListenerClass(listener);
        }
        String modeName = ci.removeProperty("MODE", null);
        if (modeName != null) {
            this.mode = Mode.getInstance(modeName);
        }
        this.multiVersion = ci.getProperty("MVCC", false);
        this.logMode = ci.getProperty("LOG", LOG_MODE_SYNC);
        boolean closeAtVmShutdown = dbSettings.dbCloseOnExit;
        int traceLevelFile = ci.getIntProperty(SetTypes.TRACE_LEVEL_FILE, TraceSystem.DEFAULT_TRACE_LEVEL_FILE);
        int traceLevelSystemOut = ci.getIntProperty(SetTypes.TRACE_LEVEL_SYSTEM_OUT, //
                TraceSystem.DEFAULT_TRACE_LEVEL_SYSTEM_OUT);
        openDatabase(traceLevelFile, traceLevelSystemOut, closeAtVmShutdown);

        initialized = true;
    }

    private void openDatabase(int traceLevelFile, int traceLevelSystemOut, boolean closeAtVmShutdown) {
        try {
            openDatabase(traceLevelFile, traceLevelSystemOut);
            if (closeAtVmShutdown) {
                try {
                    closeOnExit = new DatabaseCloser(this, 0, true);
                    Runtime.getRuntime().addShutdownHook(closeOnExit);
                } catch (IllegalStateException e) {
                    // shutdown in progress - just don't register the handler
                    // (maybe an application wants to write something into a
                    // database at shutdown time)
                } catch (SecurityException e) {
                    // applets may not do that - ignore
                    // Google App Engine doesn't allow
                    // to instantiate classes that extend Thread
                }
            }
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                e.fillInStackTrace();
            }
            if (traceSystem != null) {
                if (e instanceof SQLException) {
                    SQLException e2 = (SQLException) e;
                    if (e2.getErrorCode() != ErrorCode.DATABASE_ALREADY_OPEN_1) {
                        // only write if the database is not already in use
                        trace.error(e, "opening {0}", databaseName);
                    }
                }
                traceSystem.close();
            }
            closeOpenFilesAndUnlock(false);
            throw DbException.convert(e);
        }
    }

    private void openDatabase(int traceLevelFile, int traceLevelSystemOut) {
        if (persistent) {
            traceSystem = new TraceSystem(databaseName + Constants.SUFFIX_TRACE_FILE);
            traceSystem.setLevelFile(traceLevelFile);
            traceSystem.setLevelSystemOut(traceLevelSystemOut);
            trace = traceSystem.getTrace(Trace.DATABASE);
            trace.info("opening {0} (build {1})", databaseName, Constants.BUILD_ID);
        } else {
            traceSystem = new TraceSystem(null);
            trace = traceSystem.getTrace(Trace.DATABASE);
        }
        systemUser = new User(this, 0, SYSTEM_USER_NAME, true);
        mainSchema = new Schema(this, 0, Constants.SCHEMA_MAIN, systemUser, true);
        infoSchema = new Schema(this, -1, "INFORMATION_SCHEMA", systemUser, true);
        schemas.put(mainSchema.getName(), mainSchema);
        schemas.put(infoSchema.getName(), infoSchema);
        publicRole = new Role(this, 0, Constants.PUBLIC_ROLE_NAME, true);
        roles.put(Constants.PUBLIC_ROLE_NAME, publicRole);
        systemUser.setAdmin(true);
        systemSession = new Session(this, systemUser, ++nextSessionId);

        openMetaTable();

        if (!readOnly) {
            // set CREATE_BUILD in a new database
            String name = SetTypes.getTypeName(SetTypes.CREATE_BUILD);
            if (settings.get(name) == null) {
                Setting setting = new Setting(this, allocateObjectId(), name);
                setting.setIntValue(Constants.BUILD_ID);
                lockMeta(systemSession);
                addDatabaseObject(systemSession, setting);
            }
        }
        //getLobStorage().init();
        systemSession.commit(true);

        trace.info("opened {0}", databaseName);
        if (checkpointAllowed > 0) {
            afterWriting();
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
        data.temporary = false;
        data.persistData = persistent;
        data.persistIndexes = persistent;
        data.create = true;
        data.isHidden = true;
        data.session = systemSession;
        meta = mainSchema.createTable(data);

        IndexColumn[] pkCols = IndexColumn.wrap(new Column[] { columnId });
        IndexType indexType = IndexType.createDelegate(); //重用原有的primary index
        metaIdIndex = meta.addIndex(systemSession, "SYS_ID", 0, pkCols, indexType, true, null);

        ArrayList<MetaRecord> records = New.arrayList();
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

        recompileInvalidViews(systemSession);
        starting = false;
    }

    //TODO session参数就是systemSession，另外是否有必要来来回回recompile
    private void recompileInvalidViews(Session session) {
        boolean recompileSuccessful;
        do {
            recompileSuccessful = false;
            for (Table obj : getAllTablesAndViews(false)) {
                if (obj instanceof TableView) {
                    TableView view = (TableView) obj;
                    if (view.isInvalid()) {
                        view.recompile(session, true);
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
                if (!view.isInvalid()) {
                    view.recompile(systemSession, true);
                }
            }
        }
    }

    public static void setInitialPowerOffCount(int count) {
        initialPowerOffCount = count;
    }

    public void setPowerOffCount(int count) {
        if (powerOffCount == -1) {
            return;
        }
        powerOffCount = count;
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
    public int compareTypeSave(Value a, Value b) {
        return a.compareTypeSave(b, compareMode);
    }

    public long getModificationDataId() {
        return modificationDataId;
    }

    public long getNextModificationDataId() {
        return ++modificationDataId;
    }

    public long getModificationMetaId() {
        return modificationMetaId;
    }

    public long getNextModificationMetaId() {
        // if the meta data has been modified, the data is modified as well
        // (because MetaTable returns modificationDataId)
        modificationDataId++;
        return modificationMetaId++;
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
                checkPowerOffInternal();
                if (traceSystem != null) {
                    traceSystem.close();
                }
            } catch (DbException e) {
                TraceSystem.traceThrowable(e);
            }
        }
        getDatabaseEngine().closeDatabase(databaseName);
        throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
    }

    private void checkPowerOffInternal() {

    }

    /**
     * Check if a database with the given name exists.
     *
     * @param name the name of the database (including path)
     * @return true if one exists
     */
    static boolean exists(String name) {
        return FileUtils.exists(name + Constants.SUFFIX_MV_FILE);
    }

    /**
     * Get the trace object for the given module.
     *
     * @param module the module name
     * @return the trace object
     */
    public Trace getTrace(String module) {
        return traceSystem.getTrace(module);
    }

    @Override
    public FileStore openFile(String name, String openMode, boolean mustExist) {
        if (mustExist && !FileUtils.exists(name)) {
            throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        FileStore store = FileStore.open(this, name, openMode, cipher, filePasswordHash);
        try {
            store.init();
        } catch (DbException e) {
            store.closeSilently();
            throw e;
        }
        return store;
    }

    /**
     * Check if the file password hash is correct.
     *
     * @param testCipher the cipher algorithm
     * @param testHash the hash code
     * @return true if the cipher algorithm and the password match
     */
    boolean validateFilePasswordHash(String testCipher, byte[] testHash) {
        if (!StringUtils.equals(testCipher, this.cipher)) {
            return false;
        }
        return Utils.compareSecure(testHash, filePasswordHash);
    }

    public static String parseDatabaseShortName(DbSettings dbSettings, String databaseName) {
        String n = databaseName;
        if (n.endsWith(":")) {
            n = null;
        }
        if (n != null) {
            StringTokenizer tokenizer = new StringTokenizer(n, "/\\:,;");
            while (tokenizer.hasMoreTokens()) {
                n = tokenizer.nextToken();
            }
        }
        if (n == null || n.length() == 0) {
            n = "unnamed";
        }
        return dbSettings.databaseToUpper ? StringUtils.toUpperEnglish(n) : n;
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

    private synchronized void addMeta(Session session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && !starting && !obj.isTemporary()) {
            Row r = meta.getTemplateRow();
            MetaRecord rec = new MetaRecord(obj);
            rec.setRecord(r);
            objectIds.set(id);
            if (SysProperties.CHECK) {
                verifyMetaLocked(session);
            }
            meta.addRow(session, r);
        }
    }

    /**
     * Verify the meta table is locked.
     *
     * @param session the session
     */
    public void verifyMetaLocked(Session session) {
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
    public synchronized boolean lockMeta(Session session) {
        if (meta == null) {
            return true;
        }
        boolean wasLocked = meta.isLockedExclusivelyBy(session);
        meta.lock(session, true, true);
        return wasLocked;
    }

    /**
     * Remove the given object from the meta data.
     *
     * @param session the session
     * @param id the id of the object to remove
     */
    public synchronized void removeMeta(Session session, int id) {
        if (id > 0 && !starting) {
            SearchRow r = meta.getTemplateSimpleRow(false);
            r.setValue(0, ValueInt.get(id));
            boolean wasLocked = lockMeta(session);
            Cursor cursor = metaIdIndex.find(session, r, r);
            if (cursor.next()) {
                if (SysProperties.CHECK) {
                    if (lockMode != 0 && !wasLocked) {
                        throw DbException.throwInternalError();
                    }
                }
                Row found = cursor.get();
                meta.removeRow(session, found);
                objectIds.clear(id);
                if (SysProperties.CHECK) {
                    checkMetaFree(session, id);
                }
            } else if (!wasLocked) {
                // must not keep the lock if it was not locked
                // otherwise updating sequences may cause a deadlock
                meta.unlock(session);
                session.unlock(meta);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private HashMap<String, DbObject> getMap(int type) {
        HashMap<String, ? extends DbObject> result;
        switch (type) {
        case DbObject.USER:
            result = users;
            break;
        case DbObject.SETTING:
            result = settings;
            break;
        case DbObject.ROLE:
            result = roles;
            break;
        case DbObject.RIGHT:
            result = rights;
            break;
        case DbObject.SCHEMA:
            result = schemas;
            break;
        case DbObject.USER_DATATYPE:
            result = userDataTypes;
            break;
        case DbObject.COMMENT:
            result = comments;
            break;
        case DbObject.AGGREGATE:
            result = aggregates;
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        return (HashMap<String, DbObject>) result;
    }

    /**
     * Add a schema object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public synchronized void addSchemaObject(Session session, SchemaObject obj) {
        int id = obj.getId();
        if (id > 0 && !starting) {
            checkWritingAllowed();
        }
        lockMeta(session);
        obj.getSchema().add(obj);
        addMeta(session, obj);
    }

    /**
     * Add an object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public synchronized void addDatabaseObject(Session session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && !starting) {
            checkWritingAllowed();
        }
        HashMap<String, DbObject> map = getMap(obj.getType());
        if (obj.getType() == DbObject.USER) {
            User user = (User) obj;
            if (user.isAdmin() && systemUser.getName().equals(SYSTEM_USER_NAME)) {
                systemUser.rename(user.getName());
            }
        }
        String name = obj.getName();
        if (SysProperties.CHECK && map.get(name) != null) {
            DbException.throwInternalError("object already exists");
        }
        lockMeta(session);
        addMeta(session, obj);
        map.put(name, obj);
    }

    /**
     * Get the user defined aggregate function if it exists, or null if not.
     *
     * @param name the name of the user defined aggregate function
     * @return the aggregate function or null
     */
    public UserAggregate findAggregate(String name) {
        return aggregates.get(name);
    }

    /**
     * Get the comment for the given database object if one exists, or null if
     * not.
     *
     * @param object the database object
     * @return the comment or null
     */
    public Comment findComment(DbObject object) {
        if (object.getType() == DbObject.COMMENT) {
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
        return roles.get(roleName);
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
        return users.get(name);
    }

    /**
     * Get the user defined data type if it exists, or null if not.
     *
     * @param name the name of the user defined data type
     * @return the user defined data type or null
     */
    public UserDataType findUserDataType(String name) {
        return userDataTypes.get(name);
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
    public synchronized Session createSession(User user) {
        if (exclusiveSession != null) {
            throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
        }
        Session session = new Session(this, user, ++nextSessionId);
        userSessions.add(session);
        trace.info("connecting session #{0} to {1}", session.getId(), databaseName);
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
    public synchronized void removeSession(Session session) {
        if (session != null) {
            if (exclusiveSession == session) {
                exclusiveSession = null;
            }
            userSessions.remove(session);
            if (session != systemSession) {
                trace.info("disconnecting session #{0}", session.getId());
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
        if (session != systemSession && session != null) {
            trace.info("disconnected session #{0}", session.getId());
        }
    }

    private synchronized void closeAllSessionsException(Session except) {
        Session[] all = new Session[userSessions.size()];
        userSessions.toArray(all);
        for (Session s : all) {
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
            trace.info("closing {0} from shutdown hook", databaseName);
            closeAllSessionsException(null);
        }
        trace.info("closing {0}", databaseName);
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
            boolean lobStorageIsUsed = infoSchema.findTableOrView(systemSession, LobStorage.LOB_DATA_TABLE) != null;
            if (lobStorageIsUsed) {
                try {
                    getLobStorage();
                    lobStorage.removeAllForTable(LobStorage.TABLE_ID_SESSION_VARIABLE);
                } catch (DbException e) {
                    trace.error(e, "close");
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
                    for (SchemaObject obj : getAllSchemaObjects(DbObject.SEQUENCE)) {
                        Sequence sequence = (Sequence) obj;
                        sequence.close();
                    }
                }
                for (SchemaObject obj : getAllSchemaObjects(DbObject.TRIGGER)) {
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
                    systemSession.commit(true);
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
                Runtime.getRuntime().removeShutdownHook(closeOnExit);
            } catch (IllegalStateException e) {
                // ignore
            } catch (SecurityException e) {
                // applets may not do that - ignore
            }
            closeOnExit = null;
        }
        getDatabaseEngine().closeDatabase(databaseName);

        for (StorageEngine se : getStorageEngines())
            se.close(this);
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

    private void closeFiles() {
    }

    private void checkMetaFree(Session session, int id) {
        SearchRow r = meta.getTemplateSimpleRow(false);
        r.setValue(0, ValueInt.get(id));
        Cursor cursor = metaIdIndex.find(session, r, r);
        if (cursor.next()) {
            DbException.throwInternalError();
        }
    }

    /**
     * Allocate a new object id.
     *
     * @return the id
     */
    public synchronized int allocateObjectId() {
        int i = objectIds.nextClearBit(0);
        objectIds.set(i);
        return i;
    }

    public ArrayList<UserAggregate> getAllAggregates() {
        return New.arrayList(aggregates.values());
    }

    public ArrayList<Comment> getAllComments() {
        return New.arrayList(comments.values());
    }

    public int getAllowLiterals() {
        if (starting) {
            return Constants.ALLOW_LITERALS_ALL;
        }
        return allowLiterals;
    }

    public ArrayList<Right> getAllRights() {
        return New.arrayList(rights.values());
    }

    public ArrayList<Role> getAllRoles() {
        return New.arrayList(roles.values());
    }

    /**
     * Get all schema objects.
     *
     * @return all objects of all types
     */
    public ArrayList<SchemaObject> getAllSchemaObjects() {
        initMetaTables();
        ArrayList<SchemaObject> list = New.arrayList();
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
    public ArrayList<SchemaObject> getAllSchemaObjects(int type) {
        if (type == DbObject.TABLE_OR_VIEW) {
            initMetaTables();
        }
        ArrayList<SchemaObject> list = New.arrayList();
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
        ArrayList<Table> list = New.arrayList();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAllTablesAndViews());
        }
        return list;
    }

    public ArrayList<Schema> getAllSchemas() {
        initMetaTables();
        return New.arrayList(schemas.values());
    }

    public ArrayList<Setting> getAllSettings() {
        return New.arrayList(settings.values());
    }

    public ArrayList<UserDataType> getAllUserDataTypes() {
        return New.arrayList(userDataTypes.values());
    }

    public ArrayList<User> getAllUsers() {
        return New.arrayList(users.values());
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    @Override
    public String getDatabasePath() {
        if (persistent) {
            return FileUtils.toRealPath(databaseName);
        }
        return null;
    }

    public String getShortName() {
        return databaseShortName;
    }

    public String getName() {
        return databaseName;
    }

    /**
     * Get all sessions that are currently connected to the database.
     *
     * @param includingSystemSession if the system session should also be
     *            included
     * @return the list of sessions
     */
    public Session[] getSessions(boolean includingSystemSession) {
        ArrayList<Session> list;
        // need to synchronized on userSession, otherwise the list
        // may contain null elements
        synchronized (userSessions) {
            list = New.arrayList(userSessions);
        }
        // copy, to ensure the reference is stable
        Session sys = systemSession;
        if (includingSystemSession && sys != null) {
            list.add(sys);
        }
        Session[] array = new Session[list.size()];
        list.toArray(array);
        return array;
    }

    /**
     * Update an object in the system table.
     *
     * @param session the session
     * @param obj the database object
     */
    public synchronized void update(Session session, DbObject obj) {
        lockMeta(session);
        int id = obj.getId();
        removeMeta(session, id);
        addMeta(session, obj);
    }

    /**
     * Rename a schema object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public synchronized void renameSchemaObject(Session session, SchemaObject obj, String newName) {
        checkWritingAllowed();
        obj.getSchema().rename(obj, newName);
        updateWithChildren(session, obj);
    }

    private synchronized void updateWithChildren(Session session, DbObject obj) {
        ArrayList<DbObject> list = obj.getChildren();
        Comment comment = findComment(obj);
        if (comment != null) {
            DbException.throwInternalError();
        }
        update(session, obj);
        // remember that this scans only one level deep!
        if (list != null) {
            for (DbObject o : list) {
                if (o.getCreateSQL() != null) {
                    update(session, o);
                }
            }
        }
    }

    /**
     * Rename a database object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public synchronized void renameDatabaseObject(Session session, DbObject obj, String newName) {
        checkWritingAllowed();
        int type = obj.getType();
        HashMap<String, DbObject> map = getMap(type);
        if (SysProperties.CHECK) {
            if (!map.containsKey(obj.getName())) {
                DbException.throwInternalError("not found: " + obj.getName());
            }
            if (obj.getName().equals(newName) || map.containsKey(newName)) {
                DbException.throwInternalError("object already exists: " + newName);
            }
        }
        obj.checkRename();
        int id = obj.getId();
        lockMeta(session);
        removeMeta(session, id);
        map.remove(obj.getName());
        obj.rename(newName);
        map.put(newName, obj);
        updateWithChildren(session, obj);
    }

    /**
     * Create a temporary file in the database folder.
     *
     * @return the file name
     */
    public String createTempFile() {
        try {
            boolean inTempDir = readOnly;
            String name = databaseName;
            if (!persistent) {
                name = "memFS:" + name;
            }
            return FileUtils.createTempFile(name, Constants.SUFFIX_TEMP_FILE, true, inTempDir);
        } catch (IOException e) {
            throw DbException.convertIOException(e, databaseName);
        }
    }

    private void deleteOldTempFiles() {
        String path = FileUtils.getParent(databaseName);
        for (String name : FileUtils.newDirectoryStream(path)) {
            if (name.endsWith(Constants.SUFFIX_TEMP_FILE) && name.startsWith(databaseName)) {
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
    public synchronized void removeDatabaseObject(Session session, DbObject obj) {
        checkWritingAllowed();
        String objName = obj.getName();
        int type = obj.getType();
        HashMap<String, DbObject> map = getMap(type);
        if (SysProperties.CHECK && !map.containsKey(objName)) {
            DbException.throwInternalError("not found: " + objName);
        }
        Comment comment = findComment(obj);
        lockMeta(session);
        if (comment != null) {
            removeDatabaseObject(session, comment);
        }
        int id = obj.getId();
        obj.removeChildrenAndResources(session);
        map.remove(objName);
        removeMeta(session, id);
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
        case DbObject.COMMENT:
        case DbObject.CONSTRAINT:
        case DbObject.INDEX:
        case DbObject.RIGHT:
        case DbObject.TRIGGER:
        case DbObject.USER:
            return null;
        default:
        }
        HashSet<DbObject> set = New.hashSet();
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
    public synchronized void removeSchemaObject(Session session, SchemaObject obj) {
        int type = obj.getType();
        if (type == DbObject.TABLE_OR_VIEW) {
            Table table = (Table) obj;
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTable(table);
                return;
            }
        } else if (type == DbObject.INDEX) {
            Index index = (Index) obj;
            Table table = index.getTable();
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTableIndex(index);
                return;
            }
        } else if (type == DbObject.CONSTRAINT) {
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
        int id = obj.getId();
        if (!starting) {
            Table t = getDependentTable(obj, null);
            if (t != null) {
                obj.getSchema().add(obj);
                throw DbException.get(ErrorCode.CANNOT_DROP_2, obj.getSQL(), t.getSQL());
            }
            obj.removeChildrenAndResources(session);
        }
        removeMeta(session, id);
    }

    /**
     * Check if this database disk-based.
     *
     * @return true if it is disk-based, false it it is in-memory only.
     */
    public boolean isPersistent() {
        return persistent;
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

    //TODO 传递到存储引擎
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
        return pageSize;
    }

    public byte[] getFileEncryptionKey() {
        return fileEncryptionKey;
    }

    public synchronized void setMasterUser(User user) {
        lockMeta(systemSession);
        addDatabaseObject(systemSession, user);
        systemSession.commit(true);
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
    public synchronized String getTempTableName(String baseName, Session session) {
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

    /**
     * Get the list of in-doubt transactions.
     *
     * @return the list
     */
    public ArrayList<InDoubtTransaction> getInDoubtTransactions() {
        return null;
    }

    /**
     * Prepare a transaction.
     *
     * @param session the session
     * @param transaction the name of the transaction
     */
    synchronized void prepareCommit(Session session, String transaction) {
        if (readOnly) {
            return;
        }
    }

    /**
     * Commit the current transaction of the given session.
     *
     * @param session the session
     */
    synchronized void commit(Session session) {
        if (readOnly) {
            return;
        }
        session.setAllCommitted();
    }

    /**
     * Flush all pending changes to the transaction log.
     */
    public synchronized void flush() {
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
                String url = databaseURL;
                if (cipher != null) {
                    url += ";CIPHER=" + cipher;
                }
                eventListener.init(url);
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

    /**
     * Synchronize the files with the file system. This method is called when
     * executing the SQL statement CHECKPOINT SYNC.
     */
    public void sync() {
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
            if (multiThreaded) {
                // currently the combination of LOCK_MODE=0 and MULTI_THREADED is not supported
                throw DbException.get(ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1, "LOCK_MODE=0 & MULTI_THREADED");
            }
            break;
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

    public Session getSystemSession() {
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

    /**
     * Check if the database is currently opening. This is true until all stored
     * SQL statements have been executed.
     *
     * @return true if the database is still starting
     */
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

    public boolean isMultiThreaded() {
        return multiThreaded;
    }

    public void setMultiThreaded(boolean multiThreaded) {
        if (multiThreaded && this.multiThreaded != multiThreaded) {
            if (multiVersion) {
                // currently the combination of MVCC and MULTI_THREADED is not supported
                throw DbException.get(ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1, "MVCC & MULTI_THREADED");
            }
            if (lockMode == 0) {
                // currently the combination of LOCK_MODE=0 and MULTI_THREADED is not supported
                throw DbException.get(ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1, "LOCK_MODE=0 & MULTI_THREADED");
            }
        }
        this.multiThreaded = multiThreaded;
    }

    public void setMaxOperationMemory(int maxOperationMemory) {
        this.maxOperationMemory = maxOperationMemory;
    }

    public int getMaxOperationMemory() {
        return maxOperationMemory;
    }

    public Session getExclusiveSession() {
        return exclusiveSession;
    }

    /**
     * Set the session that can exclusively access the database.
     *
     * @param session the session
     * @param closeOthers whether other sessions are closed
     */
    public void setExclusiveSession(Session session, boolean closeOthers) {
        this.exclusiveSession = session;
        if (closeOthers) {
            closeAllSessionsException(session);
        }
    }

    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        if (lobFileListCache == null) {
            lobFileListCache = SmallLRUCache.newInstance(128);
        }
        return lobFileListCache;
    }

    /**
     * Checks if the system table (containing the catalog) is locked.
     *
     * @return true if it is currently locked
     */
    public boolean isSysTableLocked() {
        return meta == null || meta.isLockedExclusively();
    }

    public void isSysTableLockedThenUnlock(Session session) {
        if (meta != null && meta.isLockedExclusively()) {
            meta.unlock(session);
            session.unlock(meta);
        }
    }

    @Override
    public String toString() {
        return databaseShortName + ":" + super.toString();
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
        closeFiles();
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
     * Check if the contents of the database was changed and therefore it is
     * required to re-connect. This method waits until pending changes are
     * completed. If a pending change takes too long (more than 2 seconds), the
     * pending change is broken (removed from the properties file).
     *
     * @return true if reconnecting is required
     */
    public boolean isReconnectNeeded() {
        return false;
    }

    /**
     * Flush all changes when using the serialized mode, and if there are
     * pending changes, and some time has passed. This switches to a new
     * transaction log and resets the change pending flag in
     * the .lock.db file.
     */
    public void checkpointIfRequired() {
    }

    public boolean isFileLockSerialized() {
        return false;
    }

    /**
     * Flush all changes and open a new transaction log.
     */
    public void checkpoint() {
        getTempFileDeleter().deleteUnused();
    }

    /**
     * This method is called before writing to the transaction log.
     *
     * @return true if the call was successful and writing is allowed,
     *          false if another connection was faster
     */
    public boolean beforeWriting() {
        return true;
    }

    /**
     * This method is called after updates are finished.
     */
    public void afterWriting() {
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

    @Override
    public Connection getLobConnection() {
        String url = Constants.CONN_URL_INTERNAL;
        JdbcConnection conn = new JdbcConnection(systemSession, systemUser.getName(), url);
        conn.setTraceLevel(TraceSystem.OFF);
        return conn;
    }

    public void setLogMode(int log) {
        if (log < 0 || log > 2) {
            throw DbException.getInvalidValueException("LOG", log);
        }

        this.logMode = log;
    }

    public int getLogMode() {
        return logMode;
    }

    public int getDefaultTableType() {
        return defaultTableType;
    }

    public void setDefaultTableType(int defaultTableType) {
        this.defaultTableType = defaultTableType;
    }

    public void setMultiVersion(boolean multiVersion) {
        this.multiVersion = multiVersion;
    }

    public DbSettings getSettings() {
        return dbSettings;
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
        for (StorageEngine se : getStorageEngines())
            se.backupTo(this, fileName);
    }

    public Index createIndex(TableBase table, int indexId, String indexName, IndexColumn[] indexCols,
            IndexType indexType, boolean create, Session session) {
        throw DbException.throwInternalError();
    }

    private DbException backgroundException;

    public void setBackgroundException(DbException e) {
        if (backgroundException == null) {
            backgroundException = e;
            TraceSystem t = getTraceSystem();
            if (t != null) {
                t.getTrace(Trace.DATABASE).error(e, "flush");
            }
        }
    }

    //每个数据库会有多个表，每个表有可能使用不同的存储引擎
    private final List<StorageEngine> storageEngines = new CopyOnWriteArrayList<>();

    public void addStorageEngine(StorageEngine storageEngine) {
        storageEngines.add(storageEngine);
    }

    public List<StorageEngine> getStorageEngines() {
        if (storageEngines.isEmpty())
            throw new IllegalStateException("StorageEngine not init");
        return storageEngines;
    }

    //每个数据库只有一个事务引擎
    private TransactionEngine transactionEngine;

    public void setTransactionEngine(TransactionEngine transactionEngine) {
        this.transactionEngine = transactionEngine;
    }

    public TransactionEngine getTransactionEngine() {
        if (transactionEngine == null)
            throw new IllegalStateException("TransactionEngine not init");
        return transactionEngine;
    }
}
