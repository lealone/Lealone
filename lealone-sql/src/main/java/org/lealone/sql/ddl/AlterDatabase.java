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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.ServerSession;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.router.RouterHolder;
import org.lealone.storage.Storage;

/**
 * This class represents the statement
 * ALTER DATABASE
 */
public class AlterDatabase extends DatabaseStatement {

    private final Database db;
    private final Map<String, String> parameters;
    private final Map<String, String> replicationProperties;
    private final RunMode runMode;

    public AlterDatabase(ServerSession session, Database db, Map<String, String> parameters,
            Map<String, String> replicationProperties, RunMode runMode) {
        super(session);
        this.db = db;
        this.parameters = parameters;
        this.replicationProperties = replicationProperties;
        this.runMode = runMode;
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_DATABASE;
    }

    @Override
    public int update() {
        checkRight();
        synchronized (LealoneDatabase.getInstance().getLock(DbObjectType.DATABASE)) {
            RunMode oldRunMode = db.getRunMode();
            if (runMode != null)
                db.setRunMode(runMode);
            if (parameters != null)
                db.alterParameters(parameters);
            if (replicationProperties != null)
                db.setReplicationProperties(replicationProperties);

            boolean clientServer2ReplicationMode = false;
            boolean clientServer2ShardingMode = false;
            boolean replication2ShardingMode = false;
            if (oldRunMode == RunMode.CLIENT_SERVER) {
                if (runMode == RunMode.REPLICATION)
                    clientServer2ReplicationMode = true;
                else if (runMode == RunMode.SHARDING)
                    clientServer2ShardingMode = true;
            } else if (oldRunMode == RunMode.REPLICATION) {
                if (runMode == RunMode.SHARDING)
                    replication2ShardingMode = true;
            }

            String[] newHostIds = null;
            String[] oldHostIds = null;
            if (clientServer2ReplicationMode || clientServer2ShardingMode || replication2ShardingMode) {
                if (session.isRoot()) {
                    oldHostIds = db.getHostIds();
                    if (parameters != null && parameters.containsKey("hostIds")) {
                        newHostIds = StringUtils.arraySplit(parameters.get("hostIds"), ',', true);
                    } else {
                        if (clientServer2ReplicationMode)
                            newHostIds = RouterHolder.getRouter().getReplicationEndpoints(db);
                        else
                            newHostIds = RouterHolder.getRouter().getShardingEndpoints(db);
                    }

                    String hostIds = StringUtils.arrayCombine(oldHostIds, ',') + ","
                            + StringUtils.arrayCombine(newHostIds, ',');
                    db.getParameters().put("hostIds", hostIds);

                    StatementBuilder sql = new StatementBuilder("ALTER DATABASE ");
                    sql.append(db.getShortName());
                    sql.append(" RUN MODE ").append(runMode.toString());
                    if (replicationProperties != null && !replicationProperties.isEmpty()) {
                        sql.append(" WITH REPLICATION STRATEGY");
                        Database.appendMap(sql, replicationProperties);
                    }
                    sql.append(" PARAMETERS");
                    Database.appendMap(sql, db.getParameters());
                    this.sql = sql.toString();
                } else {
                    if (isTargetEndpoint(db)) {
                        oldHostIds = db.getHostIds();
                        HashSet<String> oldSet = new HashSet<>(Arrays.asList(oldHostIds));
                        if (parameters != null && parameters.containsKey("hostIds")) {
                            String[] hostIds = StringUtils.arraySplit(parameters.get("hostIds"), ',', true);
                            HashSet<String> newSet = new HashSet<>(Arrays.asList(hostIds));
                            newSet.removeAll(oldSet);
                            newHostIds = newSet.toArray(new String[0]);
                        } else {
                            DbException.throwInternalError();
                        }
                    }
                }
            }

            LealoneDatabase.getInstance().updateMeta(session, db);
            if (isTargetEndpoint(db)) {
                for (Storage storage : db.getStorages()) {
                    storage.save();
                }
                Database db2 = db.copy();
                if (clientServer2ReplicationMode) {
                    RouterHolder.getRouter().replicate(db2, oldRunMode, runMode, newHostIds);
                } else if (clientServer2ShardingMode || replication2ShardingMode) {
                    RouterHolder.getRouter().sharding(db2, oldRunMode, runMode, oldHostIds, newHostIds);
                }
            }
        }

        executeDatabaseStatement(db);
        return 0;
    }
}
