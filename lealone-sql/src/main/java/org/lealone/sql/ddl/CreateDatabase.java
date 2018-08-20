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
import org.lealone.db.value.CaseInsensitiveMap;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.router.RouterHolder;

/**
 * This class represents the statement
 * CREATE DATABASE
 */
public class CreateDatabase extends DatabaseStatement {

    private final String dbName;
    private final boolean ifNotExists;
    private final RunMode runMode;
    private final Map<String, String> replicationProperties;
    private final Map<String, String> parameters;
    // private final Map<String, String> resourceQuota;

    public CreateDatabase(ServerSession session, String dbName, boolean ifNotExists, RunMode runMode,
            Map<String, String> replicationProperties, Map<String, String> parameters) {
        super(session);
        this.dbName = dbName;
        this.ifNotExists = ifNotExists;
        this.runMode = runMode;
        this.replicationProperties = replicationProperties;
        if (parameters == null) {
            parameters = new CaseInsensitiveMap<>();
        }
        this.parameters = parameters;
        // this.resourceQuota = resourceQuota;
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_DATABASE;
    }

    @Override
    public int update() {
        checkRight(ErrorCode.CREATE_DATABASE_RIGHTS_REQUIRED);
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        Database newDB;
        synchronized (lealoneDB.getLock(DbObjectType.DATABASE)) {
            if (lealoneDB.findDatabase(dbName) != null || LealoneDatabase.NAME.equalsIgnoreCase(dbName)) {
                if (ifNotExists) {
                    return 0;
                }
                throw DbException.get(ErrorCode.DATABASE_ALREADY_EXISTS_1, dbName);
            }
            int id = getObjectId(lealoneDB);
            newDB = new Database(id, dbName, parameters);
            newDB.setReplicationProperties(replicationProperties);
            newDB.setRunMode(runMode);
            if (!parameters.containsKey("hostIds")) {
                String[] hostIds = RouterHolder.getRouter().getHostIds(newDB, false);
                if (hostIds != null && hostIds.length > 0)
                    newDB.getParameters().put("hostIds", StringUtils.arrayCombine(hostIds, ','));
                else
                    newDB.getParameters().put("hostIds", "");
            }
            if (newDB.getHostIds().length <= 1) {
                // 如果可用节点只有1个，那就退化到CLIENT_SERVER模式
                newDB.setRunMode(RunMode.CLIENT_SERVER);
                // 忽略复制参数
                newDB.setReplicationProperties(null);
            }
            lealoneDB.addDatabaseObject(session, newDB);
        }

        // LealoneDatabase在启动过程中执行CREATE DATABASE时，不对数据库初始化
        if (!lealoneDB.isStarting()) {
            executeDatabaseStatement(newDB);
            // 只有数据库真实所在的目标节点才需要初始化数据库，其他节点只需要在LealoneDatabase中有一条相应记录即可
            if (isTargetEndpoint(newDB)) {
                newDB.init();
                newDB.createRootUserIfNotExists();
            }
        }
        return 0;
    }
}
