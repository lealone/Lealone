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

    private String[] newHostIds;
    private String[] oldHostIds;

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
            if (runMode == null) {
                if (oldRunMode == RunMode.CLIENT_SERVER)
                    clientServer2ClientServer();
                else if (oldRunMode == RunMode.REPLICATION)
                    replication2Replication();
                else if (oldRunMode == RunMode.SHARDING)
                    sharding2Sharding();
            } else if (oldRunMode == RunMode.CLIENT_SERVER) {
                if (runMode == RunMode.CLIENT_SERVER)
                    clientServer2ClientServer();
                else if (runMode == RunMode.REPLICATION)
                    scaleOutClientServer2Replication();
                else if (runMode == RunMode.SHARDING)
                    scaleOutClientServer2Sharding();
            } else if (oldRunMode == RunMode.REPLICATION) {
                if (runMode == RunMode.CLIENT_SERVER)
                    scaleInReplication2ClientServer();
                else if (runMode == RunMode.REPLICATION)
                    replication2Replication();
                else if (runMode == RunMode.SHARDING)
                    scaleOutReplication2Sharding();
            } else if (oldRunMode == RunMode.SHARDING) {
                if (runMode == RunMode.CLIENT_SERVER)
                    scaleInSharding2ClientServer();
                else if (runMode == RunMode.REPLICATION)
                    scaleInSharding2Replication();
                else if (runMode == RunMode.SHARDING)
                    sharding2Sharding();
            }
        }

        updateRemoteEndpoints();
        return 0;
    }

    private void alterDatabase() {
        if (runMode != null)
            db.setRunMode(runMode);
        if (parameters != null)
            db.alterParameters(parameters);
        if (replicationProperties != null)
            db.setReplicationProperties(replicationProperties);
    }

    private void updateLocalMeta() {
        LealoneDatabase.getInstance().updateMeta(session, db);
    }

    private void updateRemoteEndpoints() {
        executeDatabaseStatement(db);
    }

    private void rewriteSql(boolean toReplicationMode) {
        if (session.isRoot()) {
            oldHostIds = db.getHostIds();
            if (parameters != null && parameters.containsKey("hostIds")) {
                newHostIds = StringUtils.arraySplit(parameters.get("hostIds"), ',', true);
            } else {
                if (toReplicationMode)
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

    private Database copyDatabase() {
        for (Storage storage : db.getStorages()) {
            storage.save();
        }
        Database db2 = db.copy();
        return db2;
    }

    private void clientServer2ClientServer() {
        alterDatabase();
        updateLocalMeta();
    }

    private void replication2Replication() {
        int replicationFactorOld = getReplicationFactor(db.getReplicationProperties());
        int replicationFactorNew = getReplicationFactor(replicationProperties);
        int value = replicationFactorNew - replicationFactorOld;
        // int replicationEndpoints = Math.abs(value);
        if (value > 0) {
            scaleOutReplication2Replication();
        } else if (value < 0) {
            scaleInReplication2Replication();
        } else {
            alterDatabase();
            updateLocalMeta();
        }
    }

    private void sharding2Sharding() {
        int nodesOld = getNodes(db.getParameters());
        int nodesNew = getNodes(parameters);
        int value = nodesNew - nodesOld;
        // int nodes = Math.abs(value);
        if (value > 0) {
            scaleOutSharding2Sharding();
        } else if (value < 0) {
            scaleInSharding2Sharding();
        } else {
            alterDatabase();
            updateLocalMeta();
        }
    }

    // ----------------------scale out----------------------

    private void scaleOutClientServer2Replication() {
        alterDatabase();
        rewriteSql(true);
        updateLocalMeta();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().replicate(db2, RunMode.CLIENT_SERVER, runMode, newHostIds);
        }
    }

    private void scaleOutClientServer2Sharding() {
        alterDatabase();
        rewriteSql(false);
        updateLocalMeta();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().sharding(db2, RunMode.CLIENT_SERVER, runMode, oldHostIds, newHostIds);
        }
    }

    private void scaleOutReplication2Sharding() {
        alterDatabase();
        rewriteSql(false);
        updateLocalMeta();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().sharding(db2, RunMode.REPLICATION, runMode, oldHostIds, newHostIds);
        }
    }

    private void scaleOutReplication2Replication() {
        alterDatabase();
        rewriteSql(true);
        updateLocalMeta();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().replicate(db2, RunMode.REPLICATION, runMode, newHostIds);
        }
    }

    private void scaleOutSharding2Sharding() {
        alterDatabase();
        rewriteSql(false);
        updateLocalMeta();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().sharding(db2, RunMode.SHARDING, runMode, oldHostIds, newHostIds);
        }
    }

    // ----------------------scale in----------------------

    private void scaleInReplication2ClientServer() {

    }

    private void scaleInSharding2ClientServer() {

    }

    private void scaleInSharding2Replication() {

    }

    private void scaleInReplication2Replication() {

    }

    private void scaleInSharding2Sharding() {

    }

    private static int getReplicationFactor(Map<String, String> replicationProperties) {
        return getIntPropertyValue("replication_factor", replicationProperties);
    }

    private static int getNodes(Map<String, String> parameters) {
        return getIntPropertyValue("nodes", parameters);
    }

    private static int getIntPropertyValue(String key, Map<String, String> properties) {
        if (properties == null)
            return 0;
        String value = properties.get(key);
        if (value == null)
            return 0;
        return Integer.parseInt(value);
    }
}
