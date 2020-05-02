/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.LockTable;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP DATABASE
 */
public class DropDatabase extends DatabaseStatement {

    private boolean ifExists;
    private boolean deleteFiles;

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

    public void setDeleteFiles(boolean deleteFiles) {
        this.deleteFiles = deleteFiles;
    }

    @Override
    public int update() {
        checkRight();
        if (LealoneDatabase.NAME.equalsIgnoreCase(dbName)) {
            throw DbException.get(ErrorCode.CANNOT_DROP_LEALONE_DATABASE);
        }
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        LockTable lockTable = lealoneDB.tryExclusiveDatabaseLock(session);
        if (lockTable == null)
            return -1;

        Database db = lealoneDB.getDatabase(dbName);
        if (db == null) {
            if (!ifExists)
                throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, dbName);
        } else {
            lealoneDB.removeDatabaseObject(session, db);
            if (isTargetNode(db)) {
                // Lealone不同于H2数据库，在H2的一个数据库中可以访问另一个数据库的对象，而Lealone不允许，
                // 所以在H2中需要一个对象一个对象地删除，这样其他数据库中的对象对他们的引用才能解除，
                // 而Lealone只要在LealoneDatabase中删除对当前数据库的引用然后删除底层的文件即可。
                if (deleteFiles) {
                    db.setDeleteFilesOnDisconnect(true);
                }
                if (db.getSessionCount() == 0) {
                    db.drop();
                }
            }
        }
        updateRemoteNodes(sql);
        return 0;
    }
}
