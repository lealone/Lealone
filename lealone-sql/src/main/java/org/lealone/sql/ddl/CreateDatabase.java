/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE DATABASE
 */
public class CreateDatabase extends DatabaseStatement {

    private final boolean ifNotExists;

    public CreateDatabase(ServerSession session, String dbName, boolean ifNotExists, RunMode runMode,
            CaseInsensitiveMap<String> parameters) {
        super(session, dbName);
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
