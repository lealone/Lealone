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

import java.util.Map;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.ServerSession;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.router.RouterHolder;

/**
 * This class represents the statement
 * CREATE DATABASE
 */
public class CreateDatabase extends DefineStatement implements DatabaseStatement {

    private final String dbName;
    private final boolean ifNotExists;
    private final RunMode runMode;
    private final Map<String, String> replicationProperties;
    // private final Map<String, String> resourceQuota;
    private final Map<String, String> parameters;

    public CreateDatabase(ServerSession session, String dbName, boolean ifNotExists, RunMode runMode,
            Map<String, String> replicationProperties, Map<String, String> parameters) {
        super(session);
        this.dbName = dbName;
        this.ifNotExists = ifNotExists;
        this.runMode = runMode;
        this.replicationProperties = replicationProperties;
        // this.resourceQuota = resourceQuota;
        this.parameters = parameters;
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_DATABASE;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        synchronized (lealoneDB.getLock(DbObjectType.DATABASE)) {
            if (lealoneDB.findDatabase(dbName) != null || LealoneDatabase.NAME.equalsIgnoreCase(dbName)) {
                if (ifNotExists) {
                    return 0;
                }
                throw DbException.get(ErrorCode.DATABASE_ALREADY_EXISTS_1, dbName);
            }
            int id = getObjectId(lealoneDB);
            Database newDB = new Database(id, dbName, parameters);
            newDB.setReplicationProperties(replicationProperties);
            newDB.setRunMode(runMode);
            if (parameters != null && !parameters.containsKey("hostIds")) {
                int[] hostIds = RouterHolder.getRouter().getHostIds(newDB);
                if (hostIds != null && hostIds.length > 0)
                    newDB.getParameters().put("hostIds", StringUtils.arrayCombine(hostIds, ','));
            }
            lealoneDB.addDatabaseObject(session, newDB);

            // LealoneDatabase在启动过程中执行CREATE DATABASE时，不对数据库初始化
            if (!lealoneDB.isStarting()) {
                newDB.init();
                newDB.createRootUserIfNotExists();
            }
        }
        return 0;

    }

    @Override
    public boolean isDatabaseStatement() {
        return true;
    }
}
