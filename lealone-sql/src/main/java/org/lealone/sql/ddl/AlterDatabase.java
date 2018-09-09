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
import java.util.TreeSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.ServerSession;
import org.lealone.net.NetEndpoint;
import org.lealone.router.RouterHolder;
import org.lealone.sql.SQLStatement;
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

            rewriteSql();
        } else {
            if (super.isTargetEndpoint(db)) {
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

    private void rewriteSql() {
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
        updateRemoteEndpoints();
    }

    private void replication2Replication() {
        int replicationFactorOld = getReplicationFactor(db.getReplicationProperties());
        int replicationFactorNew = getReplicationFactor(replicationProperties);
        int value = replicationFactorNew - replicationFactorOld;
        int replicationEndpoints = Math.abs(value);
        if (value > 0) {
            scaleOutReplication2Replication();
        } else if (value < 0) {
            scaleInReplication2Replication(replicationEndpoints);
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
            scaleInSharding2Sharding(nodesNew);
        } else {
            alterDatabase();
            updateLocalMeta();
        }
    }

    @Override
    protected boolean isTargetEndpoint(Database db) {
        // boolean isTargetEndpoint = super.isTargetEndpoint(db);
        // if (session.isRoot() && isTargetEndpoint) {
        // return true;
        // }
        TreeSet<String> hostIds = new TreeSet<>(Arrays.asList(db.getHostIds()));
        NetEndpoint localEndpoint = NetEndpoint.getLocalTcpEndpoint();
        if (hostIds.iterator().next().equalsIgnoreCase(localEndpoint.getHostAndPort())) {
            return true;
        } else {
            return false;
        }
    }

    // ----------------------scale out----------------------

    private void scaleOutClientServer2Replication() {
        alterDatabase();
        rewriteSql(true);
        updateLocalMeta();
        updateRemoteEndpoints();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().replicate(db2, RunMode.CLIENT_SERVER, runMode, newHostIds);
        }
    }

    private void scaleOutClientServer2Sharding() {
        alterDatabase();
        rewriteSql(false);
        updateLocalMeta();
        updateRemoteEndpoints();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().sharding(db2, RunMode.CLIENT_SERVER, runMode, oldHostIds, newHostIds);
        }
    }

    private void scaleOutReplication2Sharding() {
        alterDatabase();
        rewriteSql(false);
        updateLocalMeta();
        updateRemoteEndpoints();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().sharding(db2, RunMode.REPLICATION, runMode, oldHostIds, newHostIds);
        }
    }

    private void scaleOutReplication2Replication() {
        alterDatabase();
        rewriteSql(true);
        updateLocalMeta();
        updateRemoteEndpoints();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().replicate(db2, RunMode.REPLICATION, runMode, newHostIds);
        }
    }

    private void scaleOutSharding2Sharding() {
        alterDatabase();
        rewriteSql(false);
        updateLocalMeta();
        updateRemoteEndpoints();
        if (isTargetEndpoint(db)) {
            Database db2 = copyDatabase();
            RouterHolder.getRouter().sharding(db2, RunMode.SHARDING, runMode, oldHostIds, newHostIds);
        }
    }

    // ----------------------scale in----------------------

    private void scaleInReplication2ClientServer() {
        scaleInReplication2Replication(db.getHostIds().length - 1);
    }

    private void scaleInReplication2Replication(int removeReplicationEndpoints) {
        alterDatabase();
        String[] removeHostIds = null;
        if (session.isRoot()) {
            oldHostIds = db.getHostIds();
            removeHostIds = new String[removeReplicationEndpoints];
            String[] newHostIds = new String[oldHostIds.length - removeReplicationEndpoints];
            System.arraycopy(oldHostIds, 0, removeHostIds, 0, removeReplicationEndpoints);
            System.arraycopy(oldHostIds, removeReplicationEndpoints, newHostIds, 0, newHostIds.length);

            db.getParameters().put("hostIds", StringUtils.arrayCombine(newHostIds, ','));
            db.getParameters().put("removeHostIds", StringUtils.arrayCombine(removeHostIds, ','));

            rewriteSql();
        }
        updateLocalMeta();
        updateRemoteEndpoints();
        Map<String, String> parameters = db.getParameters();
        if (parameters != null && parameters.containsKey("removeHostIds")) {
            removeHostIds = StringUtils.arraySplit(parameters.get("removeHostIds"), ',', true);
            HashSet<String> set = new HashSet<>(Arrays.asList(removeHostIds));
            NetEndpoint localEndpoint = NetEndpoint.getLocalTcpEndpoint();
            if (set.contains(localEndpoint.getHostAndPort())) {
                LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
                lealoneDB.removeDatabaseObject(session, db);
                db.setDeleteFilesOnDisconnect(true);
                if (db.getSessionCount() == 0) {
                    db.drop();
                }

                Database newDB = new Database(db.getId(), db.getShortName(), parameters);
                newDB.setReplicationProperties(replicationProperties);
                newDB.setRunMode(runMode);
                lealoneDB.addDatabaseObject(session, newDB);
                newDB.notifyRunModeChanged();
            }
        }
    }

    private void scaleInSharding2ClientServer() {
        scaleInSharding(1, RunMode.CLIENT_SERVER);
    }

    private void scaleInSharding2Replication() {
        int replicationFactor = getReplicationFactor(db.getReplicationProperties());
        if (replicationProperties != null) {
            int rf = getReplicationFactor(replicationProperties);
            if (rf < replicationFactor)
                replicationFactor = rf;
        }

        scaleInSharding(replicationFactor, RunMode.REPLICATION);
    }

    private void scaleInSharding2Sharding(int nodes) {
        scaleInSharding(nodes, RunMode.SHARDING);
    }

    private void scaleInSharding(int scaleInNodes, RunMode newRunMode) {
        RunMode oldRunMode = RunMode.SHARDING;
        String[] removeHostIds = null;
        alterDatabase();
        if (session.isRoot()) {
            oldHostIds = db.getHostIds();
            removeHostIds = new String[oldHostIds.length - scaleInNodes];
            String[] newHostIds = new String[scaleInNodes];
            System.arraycopy(oldHostIds, 0, removeHostIds, 0, removeHostIds.length);
            System.arraycopy(oldHostIds, removeHostIds.length, newHostIds, 0, scaleInNodes);

            db.getParameters().put("hostIds", StringUtils.arrayCombine(newHostIds, ','));
            db.getParameters().put("removeHostIds", StringUtils.arrayCombine(removeHostIds, ','));
            db.getParameters().put("nodes", newHostIds.length + "");
            rewriteSql();
        }
        updateLocalMeta();
        updateRemoteEndpoints();
        Map<String, String> parameters = db.getParameters();

        newHostIds = StringUtils.arraySplit(parameters.get("hostIds"), ',', true);
        HashSet<String> set;
        String localHostId = NetEndpoint.getLocalTcpEndpoint().getHostAndPort();
        if (parameters != null && parameters.containsKey("removeHostIds")) {
            removeHostIds = StringUtils.arraySplit(parameters.get("removeHostIds"), ',', true);
            set = new HashSet<>(Arrays.asList(removeHostIds));
            if (set.contains(localHostId)) {
                // TODO 等到数据迁移完成后再删
                // LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
                // lealoneDB.removeDatabaseObject(session, db);
                // db.setDeleteFilesOnDisconnect(true);
                // if (db.getSessionCount() == 0) {
                // db.drop();
                // }
                //
                // Database newDB = new Database(db.getId(), db.getShortName(), parameters);
                // newDB.setReplicationProperties(replicationProperties);
                // newDB.setRunMode(runMode);
                // lealoneDB.addDatabaseObject(session, newDB);
            }
        }

        if (newRunMode == RunMode.SHARDING) {
            set = new HashSet<>(Arrays.asList(removeHostIds));
            if (set.contains(localHostId)) {
                Database db2 = copyDatabase();
                RouterHolder.getRouter().scaleIn(db2, oldRunMode, newRunMode, removeHostIds, newHostIds);
            }
        } else if (newRunMode == RunMode.CLIENT_SERVER || newRunMode == RunMode.REPLICATION) {
            set = new HashSet<>(Arrays.asList(newHostIds));
            if (set.contains(localHostId)) {
                Database db2 = copyDatabase();
                RouterHolder.getRouter().scaleIn(db2, oldRunMode, newRunMode, null, newHostIds);
            }
        }
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
