/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP DATABASE
 */
public class DropDatabase extends DatabaseStatement {

    private boolean ifExists;

    public DropDatabase(ServerSession session, String dbName) {
        super(session, dbName);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_DATABASE;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "drop database");
        if (LealoneDatabase.isMe(dbName)) {
            throw DbException.get(ErrorCode.CANNOT_DROP_LEALONE_DATABASE);
        }
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        DbObjectLock lock = lealoneDB.tryExclusiveDatabaseLock(session);
        if (lock == null)
            return -1;

        Database db = lealoneDB.getDatabase(dbName);
        if (db == null) {
            if (!ifExists)
                throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, dbName);
        } else {
            lealoneDB.removeDatabaseObject(session, db, lock);
            // Lealone不同于H2数据库，在H2的一个数据库中可以访问另一个数据库的对象，而Lealone不允许，
            // 所以在H2中需要一个对象一个对象地删除，这样其他数据库中的对象对他们的引用才能解除，
            // 而Lealone只要在LealoneDatabase中删除对当前数据库的引用然后删除底层的文件即可。
            db.markClosed();
            db.drop();
        }
        return 0;
    }
}
