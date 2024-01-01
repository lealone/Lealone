/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.db.Database;
import com.lealone.db.DbSetting;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.Mode;
import com.lealone.db.RunMode;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE DATABASE
 */
public class CreateDatabase extends DatabaseStatement {

    private final boolean ifNotExists;

    public CreateDatabase(ServerSession session, String dbName, boolean ifNotExists, RunMode runMode,
            CaseInsensitiveMap<String> parameters) {
        super(session, dbName);
        if (parameters == null)
            parameters = new CaseInsensitiveMap<>();
        this.ifNotExists = ifNotExists;
        this.parameters = parameters;
        validateParameters();
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_DATABASE;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "create database");
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        DbObjectLock lock = lealoneDB.tryExclusiveDatabaseLock(session);
        if (lock == null)
            return -1;

        if (LealoneDatabase.isMe(dbName) || lealoneDB.findDatabase(dbName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.DATABASE_ALREADY_EXISTS_1, dbName);
        }
        // 设置默认SQL引擎
        if (!parameters.containsKey(DbSetting.DEFAULT_SQL_ENGINE.name())) {
            Mode mode = session.getDatabase().getMode();
            if (mode.isMySQL()) {
                parameters.put(DbSetting.DEFAULT_SQL_ENGINE.name(), mode.getName());
            }
        }
        int id = getObjectId(lealoneDB);
        Database newDB = new Database(id, dbName, parameters);
        newDB.setRunMode(RunMode.CLIENT_SERVER);
        lealoneDB.addDatabaseObject(session, newDB, lock);
        // 将缓存过期掉
        lealoneDB.getNextModificationMetaId();

        // LealoneDatabase在启动过程中执行CREATE DATABASE时，不对数据库初始化
        if (!lealoneDB.isStarting()) {
            newDB.init();
            newDB.createRootUserIfNotExists();
        }
        return 0;
    }
}
