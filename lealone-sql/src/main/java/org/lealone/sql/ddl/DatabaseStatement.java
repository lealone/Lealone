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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.Database;
import org.lealone.db.DbSettings;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.net.NetNode;
import org.lealone.net.NetNodeManager;
import org.lealone.net.NetNodeManagerHolder;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.replication.ReplicationSession;

//CREATE/ALTER/DROP DATABASE语句在所有节点上都会执行一次，
//差别是数据库所在节点会执行更多操作，其他节点只在LealoneDatabase中有一条相应记录，
//这样客户端在接入集群的任何节点时都能找到所连数据库所在的节点有哪些。
public abstract class DatabaseStatement extends DefinitionStatement {

    protected final String dbName;
    protected RunMode runMode;
    protected CaseInsensitiveMap<String> parameters;
    protected CaseInsensitiveMap<String> replicationParameters;
    protected CaseInsensitiveMap<String> nodeAssignmentParameters;

    protected DatabaseStatement(ServerSession session, String dbName) {
        super(session);
        this.dbName = dbName;
    }

    @Override
    public boolean isDatabaseStatement() {
        return true;
    }

    protected void checkRight() {
        checkRight(null);
    }

    protected void checkRight(Integer errorCode) {
        // 只有用管理员连接到LealoneDatabase才能执行CREATE/ALTER/DROP DATABASE语句
        if (!(LealoneDatabase.getInstance() == session.getDatabase() && session.getUser().isAdmin())) {
            if (errorCode != null)
                throw DbException.get(errorCode.intValue());
            else
                throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                        "create/alter/drop database only allowed for the super user");
        }
    }

    protected boolean isTargetNode(Database db) {
        NetNode localNode = NetNode.getLocalTcpNode();
        return db.isTargetNode(localNode);
    }

    protected void updateRemoteNodes(String sql) {
        // 只有接入节点才适合把当前的SQL转到其他节点上执行
        if (!session.isRoot())
            return;
        NetNodeManager m = NetNodeManagerHolder.get();
        Set<NetNode> liveMembers = m.getLiveNodes();
        NetNode localNode = NetNode.getLocalP2pNode();
        liveMembers.remove(localNode);
        if (liveMembers.isEmpty())
            return;
        ReplicationSession rs = Database.createReplicationSession(session, liveMembers, true, null);
        try (SQLCommand c = rs.createSQLCommand(sql, -1)) {
            c.executeUpdate().get();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    protected void validateParameters() {
        if (this.parameters == null)
            this.parameters = new CaseInsensitiveMap<>();

        // 第一步: 验证可识别的参数
        CaseInsensitiveMap<String> parameters = new CaseInsensitiveMap<>(this.parameters);
        String replicationStrategy = parameters.get("replication_strategy");
        String nodeAssignmentStrategy = parameters.get("node_assignment_strategy");
        removeParameters(parameters);

        Collection<String> recognizedReplicationStrategyOptions = NetNodeManagerHolder.get()
                .getRecognizedReplicationStrategyOptions(replicationStrategy);
        if (recognizedReplicationStrategyOptions == null) {
            recognizedReplicationStrategyOptions = new HashSet<>(1);
        } else {
            recognizedReplicationStrategyOptions = new HashSet<>(recognizedReplicationStrategyOptions);
        }
        recognizedReplicationStrategyOptions.add("replication_strategy");

        Collection<String> recognizedNodeAssignmentStrategyOptions = NetNodeManagerHolder.get()
                .getRecognizedNodeAssignmentStrategyOptions(nodeAssignmentStrategy);

        if (recognizedNodeAssignmentStrategyOptions == null) {
            recognizedNodeAssignmentStrategyOptions = new HashSet<>(1);
        } else {
            recognizedNodeAssignmentStrategyOptions = new HashSet<>(recognizedNodeAssignmentStrategyOptions);
        }
        recognizedNodeAssignmentStrategyOptions.add("node_assignment_strategy");

        Collection<String> recognizedSettingOptions = DbSettings.getDefaultSettings().getSettings().keySet();
        parameters.removeAll(recognizedReplicationStrategyOptions);
        parameters.removeAll(recognizedNodeAssignmentStrategyOptions);
        parameters.removeAll(recognizedSettingOptions);
        if (!parameters.isEmpty()) {
            throw new ConfigException(String.format("Unrecognized parameters: %s for database %s, " //
                    + "recognized replication strategy options: %s, " //
                    + "node assignment strategy options: %s, " //
                    + "database setting options: %s", //
                    parameters.keySet(), dbName, recognizedReplicationStrategyOptions,
                    recognizedNodeAssignmentStrategyOptions, recognizedSettingOptions));
        }

        // 第二步: 初始化replicationParameters、nodeAssignmentParameters、database settings
        parameters = new CaseInsensitiveMap<>(this.parameters);
        removeParameters(parameters);

        replicationParameters = new CaseInsensitiveMap<>();
        if (!parameters.containsKey("replication_factor")) {
            replicationParameters.put("replication_factor",
                    NetNodeManagerHolder.get().getDefaultReplicationFactor() + "");
        }
        if (replicationStrategy != null) {
            replicationParameters.put("class", replicationStrategy);
        } else {
            replicationParameters.put("class", NetNodeManagerHolder.get().getDefaultReplicationStrategy());
        }
        for (String option : recognizedReplicationStrategyOptions) {
            if (parameters.containsKey(option))
                replicationParameters.put(option, parameters.get(option));
        }

        nodeAssignmentParameters = new CaseInsensitiveMap<>();
        if (!parameters.containsKey("assignment_factor")) {
            nodeAssignmentParameters.put("assignment_factor",
                    NetNodeManagerHolder.get().getDefaultNodeAssignmentFactor() + "");
        }
        if (nodeAssignmentStrategy != null) {
            nodeAssignmentParameters.put("class", nodeAssignmentStrategy);
        } else {
            nodeAssignmentParameters.put("class", NetNodeManagerHolder.get().getDefaultNodeAssignmentStrategy());
        }
        for (String option : recognizedNodeAssignmentStrategyOptions) {
            if (parameters.containsKey(option))
                nodeAssignmentParameters.put(option, parameters.get(option));
        }
        if (runMode == RunMode.CLIENT_SERVER) {
            nodeAssignmentParameters.put("assignment_factor", "1");
        } else if (runMode == RunMode.REPLICATION) {
            nodeAssignmentParameters.put("assignment_factor", replicationParameters.get("replication_factor"));
        } else if (runMode == RunMode.SHARDING) {
            if (!parameters.containsKey("assignment_factor"))
                nodeAssignmentParameters.put("assignment_factor", replicationParameters.get("replication_factor"));
        }
        // parameters剩下的当成database setting
    }

    private static void removeParameters(CaseInsensitiveMap<String> parameters) {
        parameters.remove("hostIds");
        parameters.remove("replication_strategy");
        parameters.remove("node_assignment_strategy");
        parameters.remove("_operationNode_");
        parameters.remove("_removeHostIds_");
    }
}
