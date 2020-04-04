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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.session.ServerSession;
import org.lealone.net.NetNode;
import org.lealone.net.NetNodeManagerHolder;
import org.lealone.sql.SQLStatement;
import org.lealone.storage.Storage;

/**
 * This class represents the statement
 * ALTER DATABASE
 */
public class AlterDatabase extends DatabaseStatement {

    private final Database db;
    private String[] newHostIds;
    private String[] oldHostIds;

    public AlterDatabase(ServerSession session, Database db, RunMode runMode, CaseInsensitiveMap<String> parameters) {
        super(session, db.getName());
        this.db = db;
        this.runMode = runMode;
        // 先使用原有的参数，然后再用新的覆盖
        this.parameters = new CaseInsensitiveMap<>(db.getParameters());
        if (parameters != null && !parameters.isEmpty()) {
            this.parameters.putAll(parameters);
            validateParameters();
        }
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
        if (replicationParameters != null)
            db.setReplicationParameters(replicationParameters);
        if (nodeAssignmentParameters != null)
            db.setNodeAssignmentParameters(nodeAssignmentParameters);
    }

    private void updateLocalMeta() {
        LealoneDatabase.getInstance().updateMeta(session, db);
    }

    private void updateRemoteNodes() {
        executeDatabaseStatement(db);
    }

    private void assignNodes() {
        assignOperationNode();
        if (session.isRoot()) {
            oldHostIds = db.getHostIds();
            if (parameters != null && parameters.containsKey("hostIds")) {
                newHostIds = StringUtils.arraySplit(parameters.get("hostIds"), ',', true);
            } else {
                newHostIds = NetNodeManagerHolder.get().assignNodes(db);
            }

            String[] hostIds = new String[oldHostIds.length + newHostIds.length];
            System.arraycopy(oldHostIds, 0, hostIds, 0, oldHostIds.length);
            System.arraycopy(newHostIds, 0, hostIds, oldHostIds.length, newHostIds.length);
            db.setHostIds(hostIds);

            rewriteSql();
        } else {
            if (isTargetNode(db)) {
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

    private void assignOperationNode() {
        // 由接入节点选择哪个节点作为发起数据复制操作的节点
        if (session.isRoot()) {
            String operationNode = null;
            if (isTargetNode(db)) {
                operationNode = db.getHostId(NetNode.getLocalP2pNode());
            } else {
                Set<NetNode> liveMembers = NetNodeManagerHolder.get().getLiveNodes();
                for (String hostId : db.getHostIds()) {
                    if (liveMembers.contains(db.getNode(hostId))) {
                        operationNode = hostId;
                        break;
                    }
                }
            }
            if (operationNode != null) {
                db.getParameters().put("_operationNode_", operationNode);
            }
        }
    }

    // 判断当前节点是否是发起数据复制操作的节点
    private boolean isOperationNode(Database db) {
        // 如果当前节点是接入节点并且是数据库所在的目标节点之一，
        // 那么当前节点就会被选为发起数据复制操作的节点
        if (session.isRoot()) {
            if (isTargetNode(db)) {
                return true;
            } else {
                return false;
            }
        } else {
            // 看看当前节点是不是被选中的节点
            String operationNode = parameters.get("_operationNode_");
            if (operationNode != null) {
                NetNode node = db.getNode(operationNode);
                if (node.equals(NetNode.getLocalP2pNode())) {
                    return true;
                }
            }
            return false;
        }
    }

    private void rewriteSql() {
        StatementBuilder sql = new StatementBuilder("ALTER DATABASE ");
        sql.append(db.getShortName());
        sql.append(" RUN MODE ").append(runMode.toString());
        sql.append(" PARAMETERS");
        Database.appendMap(sql, db.getParameters());
        this.sql = sql.toString();
    }

    // ----------------------同级操作----------------------

    // 只在所有节点上执行原始的ALTER DATABASE语句，不需要加减节点
    private void updateAllNodes() {
        alterDatabase();
        updateLocalMeta();
        updateRemoteNodes();
    }

    private void clientServer2ClientServer() {
        updateAllNodes();
    }

    private void replication2Replication() {
        int oldReplicationFactor = getReplicationFactor(db.getReplicationParameters());
        int newReplicationFactor = getReplicationFactor(replicationParameters);
        int value = newReplicationFactor - oldReplicationFactor;
        if (value > 0) {
            scaleOutReplication2Replication();
        } else if (value < 0) {
            int removeReplicationNodes = Math.abs(value);
            scaleInReplication2Replication(removeReplicationNodes);
        } else {
            updateAllNodes();
        }
    }

    private void sharding2Sharding() {
        int oldAssignmentFactor = getAssignmentFactor(db.getNodeAssignmentParameters());
        int newAssignmentFactor = getAssignmentFactor(nodeAssignmentParameters);
        int value = newAssignmentFactor - oldAssignmentFactor;
        if (value > 0) {
            scaleOutSharding2Sharding();
        } else if (value < 0) {
            scaleInSharding2Sharding(newAssignmentFactor);
        } else {
            updateAllNodes();
        }
    }

    // ----------------------scale out----------------------

    private void scaleOutClientServer2Replication() {
        scaleOut(false);
    }

    private void scaleOutClientServer2Sharding() {
        scaleOut(true);
    }

    private void scaleOutReplication2Sharding() {
        scaleOut(true);
    }

    private void scaleOutReplication2Replication() {
        scaleOut(false);
    }

    private void scaleOutSharding2Sharding() {
        scaleOut(true);
    }

    private void scaleOut(boolean isSharding) {
        alterDatabase();
        assignNodes();
        updateLocalMeta();
        updateRemoteNodes();
        if (isOperationNode(db)) {
            String taskName = isSharding ? "Sharding Pages" : "Replicate Pages";
            ConcurrentUtils.submitTask(taskName, () -> {
                // 先复制包含meta(sys)表的Storage
                Storage metaStorage = db.getMetaStorage();
                List<Storage> storages = db.getStorages();
                storages.remove(metaStorage);
                storages.add(0, metaStorage);
                for (Storage storage : storages) {
                    if (isSharding)
                        storage.sharding(db, oldHostIds, newHostIds, runMode);
                    else
                        storage.replicateTo(db, newHostIds, runMode);
                }
                db.notifyRunModeChanged();
            });
        }
    }

    // ----------------------scale in----------------------

    private void scaleInReplication2ClientServer() {
        scaleInReplication(db.getHostIds().length - 1); // 只留一个节点
    }

    private void scaleInReplication2Replication(int removeReplicationNodes) {
        scaleInReplication(removeReplicationNodes);
    }

    private void scaleInReplication(int removeReplicationNodes) {
        alterDatabase();
        String[] removeHostIds = null;
        if (session.isRoot()) {
            oldHostIds = db.getHostIds();
            removeHostIds = new String[removeReplicationNodes];
            String[] newHostIds = new String[oldHostIds.length - removeReplicationNodes];
            System.arraycopy(oldHostIds, 0, removeHostIds, 0, removeReplicationNodes);
            System.arraycopy(oldHostIds, removeReplicationNodes, newHostIds, 0, newHostIds.length);

            db.setHostIds(newHostIds);
            db.getParameters().put("_removeHostIds_", StringUtils.arrayCombine(removeHostIds, ','));

            rewriteSql();
        } else {
            if (parameters != null && parameters.containsKey("_removeHostIds_")) {
                removeHostIds = StringUtils.arraySplit(parameters.get("_removeHostIds_"), ',', true);
            }
        }
        updateLocalMeta();
        updateRemoteNodes();

        if (removeHostIds != null) {
            HashSet<String> set = new HashSet<>(Arrays.asList(removeHostIds));
            NetNode localNode = NetNode.getLocalTcpNode();
            if (set.contains(localNode.getHostAndPort())) {
                rebuildDatabase();
                db.notifyRunModeChanged();
            }
        }
    }

    private Database rebuildDatabase() {
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        lealoneDB.removeDatabaseObject(session, db);
        db.setDeleteFilesOnDisconnect(true);
        if (db.getSessionCount() == 0) {
            db.drop();
        }

        Database newDB = new Database(db.getId(), db.getShortName(), parameters);
        newDB.setReplicationParameters(replicationParameters);
        newDB.setNodeAssignmentParameters(nodeAssignmentParameters);
        newDB.setRunMode(runMode);
        lealoneDB.addDatabaseObject(session, newDB);
        return newDB;
    }

    private void scaleInSharding2ClientServer() {
        scaleInSharding(1);
    }

    private void scaleInSharding2Replication() {
        int oldReplicationFactor = getReplicationFactor(db.getReplicationParameters());
        if (replicationParameters != null) {
            int newReplicationFactor = getReplicationFactor(replicationParameters);
            if (newReplicationFactor < oldReplicationFactor)
                oldReplicationFactor = newReplicationFactor;
        }

        scaleInSharding(oldReplicationFactor);
    }

    private void scaleInSharding2Sharding(int assignmentFactor) {
        scaleInSharding(assignmentFactor);
    }

    private void scaleInSharding(int newAssignmentFactor) {
        alterDatabase();
        if (session.isRoot()) {
            oldHostIds = db.getHostIds();
            String[] removeHostIds = new String[oldHostIds.length - newAssignmentFactor];
            newHostIds = new String[newAssignmentFactor];
            System.arraycopy(oldHostIds, 0, removeHostIds, 0, removeHostIds.length);
            System.arraycopy(oldHostIds, removeHostIds.length, newHostIds, 0, newAssignmentFactor);
            oldHostIds = removeHostIds;

            db.setHostIds(newHostIds);
            db.getParameters().put("_removeHostIds_", StringUtils.arrayCombine(removeHostIds, ','));
            db.getParameters().put("hostIds", newHostIds.length + "");
            rewriteSql();
        } else {
            if (parameters == null || !parameters.containsKey("_removeHostIds_") || !parameters.containsKey("hostIds"))
                DbException.throwInternalError();
            oldHostIds = StringUtils.arraySplit(parameters.get("_removeHostIds_"), ',', true);
            newHostIds = StringUtils.arraySplit(parameters.get("hostIds"), ',', true);
        }

        updateLocalMeta();
        updateRemoteNodes();

        if (isTargetNode(db)) {
            ConcurrentUtils.submitTask("ScaleIn Nodes", () -> {
                for (Storage storage : db.getStorages()) {
                    storage.scaleIn(db, RunMode.SHARDING, runMode, oldHostIds, newHostIds);
                }
                HashSet<String> set = new HashSet<>(Arrays.asList(oldHostIds));
                String localHostId = NetNode.getLocalTcpNode().getHostAndPort();
                if (set.contains(localHostId)) {
                    // 等到数据迁移完成后再删
                    rebuildDatabase();
                }
                db.notifyRunModeChanged();
            });
        }
    }

    private static int getReplicationFactor(Map<String, String> parameters) {
        return getIntValue("replication_factor", parameters);
    }

    private static int getAssignmentFactor(Map<String, String> parameters) {
        return getIntValue("assignment_factor", parameters);
    }

    private static int getIntValue(String key, Map<String, String> parameters) {
        if (parameters == null)
            return 0;
        String value = parameters.get(key);
        if (value == null)
            return 0;
        return Integer.parseInt(value);
    }
}
