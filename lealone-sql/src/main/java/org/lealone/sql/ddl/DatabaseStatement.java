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
import java.util.Map;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.Database;
import org.lealone.db.DbSettings;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.NetNode;
import org.lealone.net.NetNodeManagerHolder;
import org.lealone.sql.router.SQLRouter;

public abstract class DatabaseStatement extends DefinitionStatement {

    protected final String dbName;
    protected RunMode runMode;
    protected Map<String, String> parameters;
    protected Map<String, String> replicationProperties;
    protected Map<String, String> nodeAssignmentProperties;

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

    protected void executeDatabaseStatement(Database db) {
        if (session.isRoot()) {
            SQLRouter.executeDatabaseStatement(db, session, this);
        }
    }

    protected void validateParameters() {
        CaseInsensitiveMap<String> parameters = new CaseInsensitiveMap<>(this.parameters);
        parameters.remove("hostIds");
        String replicationStrategy = parameters.get("replication_strategy");
        String nodeAssignmentStrategy = parameters.get("node_assignment_strategy");

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

        parameters = (CaseInsensitiveMap<String>) this.parameters;
        replicationProperties = new CaseInsensitiveMap<>();
        if (!parameters.containsKey("replication_factor")) {
            replicationProperties.put("replication_factor",
                    NetNodeManagerHolder.get().getDefaultReplicationFactor() + "");
        }
        if (replicationStrategy != null) {
            replicationProperties.put("class", replicationStrategy);
        } else {
            replicationProperties.put("class", NetNodeManagerHolder.get().getDefaultReplicationStrategy());
        }
        for (String option : recognizedReplicationStrategyOptions) {
            if (parameters.containsKey(option))
                replicationProperties.put(option, parameters.get(option));
        }

        nodeAssignmentProperties = new CaseInsensitiveMap<>();
        if (!parameters.containsKey("assignment_factor")) {
            nodeAssignmentProperties.put("assignment_factor",
                    NetNodeManagerHolder.get().getDefaultNodeAssignmentFactor() + "");
        }
        if (nodeAssignmentStrategy != null) {
            nodeAssignmentProperties.put("class", nodeAssignmentStrategy);
        } else {
            nodeAssignmentProperties.put("class", NetNodeManagerHolder.get().getDefaultNodeAssignmentStrategy());
        }
        for (String option : recognizedNodeAssignmentStrategyOptions) {
            if (parameters.containsKey(option))
                nodeAssignmentProperties.put(option, parameters.get(option));
        }
        if (runMode == RunMode.CLIENT_SERVER) {
            nodeAssignmentProperties.put("assignment_factor", "1");
        } else if (runMode == RunMode.REPLICATION) {
            nodeAssignmentProperties.put("assignment_factor", replicationProperties.get("replication_factor"));
        } else if (runMode == RunMode.SHARDING) {
            if (!parameters.containsKey("assignment_factor"))
                nodeAssignmentProperties.put("assignment_factor", replicationProperties.get("replication_factor"));
            // throw new ConfigException("In sharding mode, assignment_factor must be set");
        }
        // parameters剩下的当成database setting
    }
}
