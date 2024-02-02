/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.transaction.TransactionEngine;

/**
 * 最顶层的数据库，用于管理所有应用创建的数据库
 * 
 * @author zhh
 */
public class LealoneDatabase extends Database
        implements com.lealone.transaction.TransactionEngine.GcTask {

    // ID固定为0
    public static final int ID = 0;
    public static final String NAME = Constants.PROJECT_NAME;

    // 仅用于支持qinsql项目
    private static final CaseInsensitiveMap<String> UNSUPPORTED_SCHEMA_MAP = new CaseInsensitiveMap<>();
    private static final CaseInsensitiveMap<Object[]> CLOSED_DATABASES = new CaseInsensitiveMap<>();
    private static LealoneDatabase INSTANCE = new LealoneDatabase();

    public static LealoneDatabase getInstance() {
        return INSTANCE;
    }

    public static void addUnsupportedSchema(String schemaName) {
        UNSUPPORTED_SCHEMA_MAP.put(schemaName, schemaName);
    }

    public static boolean isUnsupportedSchema(String schemaName) {
        return UNSUPPORTED_SCHEMA_MAP.containsKey(schemaName);
    }

    public static boolean isMe(String dbName) {
        return LealoneDatabase.NAME.equalsIgnoreCase(dbName);
    }

    private LealoneDatabase() {
        super(ID, NAME, null);

        // init执行过程中会触发getInstance()，此时INSTANCE为null，会导致NPE
        INSTANCE = this;
        // 把自己也加进去，这样通过lealone这个名字能找到自己
        addDatabaseObject(null, this, null);

        init();
        createRootUserIfNotExists();
        getTransactionEngine().addGcTask(this);
    }

    public synchronized Database createEmbeddedDatabase(String name, ConnectionInfo ci) {
        Database db = findDatabase(name);
        if (db != null)
            return db;

        HashMap<String, String> parameters = new HashMap<>();
        for (Entry<Object, Object> e : ci.getProperties().entrySet()) {
            parameters.put(e.getKey().toString(), e.getValue().toString());
        }
        int id = ci.getDatabaseId() < 0 ? INSTANCE.allocateObjectId() : ci.getDatabaseId();
        db = new Database(id, name, parameters);
        db.setRunMode(RunMode.EMBEDDED);
        db.init();
        String userName = ci.getUserName();
        byte[] userPasswordHash = ci.getUserPasswordHash();
        db.createAdminUser(userName, userPasswordHash);
        // 新建session，避免使用system session
        try (ServerSession session = createSession(getSystemUser())) {
            DbObjectLock lock = tryExclusiveDatabaseLock(session);
            addDatabaseObject(session, db, lock);
            session.commit();
        }
        return db;
    }

    void closeDatabase(String dbName) {
        Database db = findDatabase(dbName);
        if (db != null) {
            synchronized (CLOSED_DATABASES) {
                getDatabasesMap().remove(dbName); // 要放到同步块中
                CLOSED_DATABASES.put(dbName, new Object[] { db.getCreateSQL(), db.getId() });
            }
        }
    }

    void dropDatabase(String dbName) {
        synchronized (CLOSED_DATABASES) {
            CLOSED_DATABASES.remove(dbName);
        }
    }

    Map<String, Database> getDatabasesMap() {
        return getDbObjects(DbObjectType.DATABASE);
    }

    public List<Database> getDatabases() {
        synchronized (CLOSED_DATABASES) {
            return new ArrayList<>(getDatabasesMap().values());
        }
    }

    public Database findDatabase(String dbName) {
        return find(DbObjectType.DATABASE, null, dbName);
    }

    /**
     * Get database with the given name.
     * This method throws an exception if the database does not exist.
     * 
     * @param name the database name
     * @return the database
     * @throws DbException if the database does not exist
     */
    public Database getDatabase(String dbName) {
        Database db = findDatabase(dbName);
        if (db == null) {
            synchronized (CLOSED_DATABASES) {
                Object[] a = CLOSED_DATABASES.remove(dbName);
                if (a != null) {
                    MetaRecord.execute(this, getSystemSession(), getEventListener(), (String) a[0],
                            (int) a[1]);
                    db = findDatabase(dbName);
                }
            }
        }
        if (db == null) {
            throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, dbName);
        }
        return db;
    }

    public boolean isClosed(String dbName) {
        synchronized (CLOSED_DATABASES) {
            return CLOSED_DATABASES.containsKey(dbName);
        }
    }

    @Override
    public synchronized Database copy() {
        INSTANCE = new LealoneDatabase();
        getTransactionEngine().removeGcTask(this);
        return INSTANCE;
    }

    // 只有用管理员连接到LealoneDatabase才能执行某些语句，比如CREATE/ALTER/DROP DATABASE
    public static void checkAdminRight(ServerSession session, String stmt) {
        if (!isSuperAdmin(session))
            throw DbException.get(ErrorCode.LEALONE_DATABASE_ADMIN_RIGHT_1, stmt);
    }

    public static boolean isSuperAdmin(ServerSession session) {
        Database db = session.getDatabase();
        // 在MySQL兼容模式下只需要当前用户是管理员即可
        if (db.getMode().isMySQL())
            return session.getUser().isAdmin();
        else
            return LealoneDatabase.getInstance() == db && session.getUser().isAdmin();
    }

    @Override
    public void gc(TransactionEngine te) {
        // getDatabases()会copy一份，因为closeIfNeeded()可能会关闭数据库，避免ConcurrentModificationException
        for (Database db : getDatabases()) {
            long metaId = db.getModificationMetaId();
            if (metaId == db.getLastGcMetaId())
                continue;
            db.setLastGcMetaId(metaId);

            // 数据库没有进行初始化时不进行GC
            if (!db.isInitialized())
                continue;
            if (db.getSessionCount() == 0 && db.closeIfNeeded())
                continue;
            for (TransactionalDbObjects tObjects : db.getTransactionalDbObjectsArray()) {
                if (tObjects != null)
                    tObjects.gc(te);
            }
            for (Schema schema : db.getAllSchemas()) {
                for (TransactionalDbObjects tObjects : schema.getTransactionalDbObjectsArray()) {
                    if (tObjects != null)
                        tObjects.gc(te);
                }
            }
        }
    }
}
