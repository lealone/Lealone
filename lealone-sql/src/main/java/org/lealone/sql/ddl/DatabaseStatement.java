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
import org.lealone.net.NetEndpoint;
import org.lealone.net.NetEndpointManagerHolder;
import org.lealone.sql.router.SQLRouter;

public abstract class DatabaseStatement extends DefinitionStatement {

    protected final String dbName;
    protected RunMode runMode;
    protected Map<String, String> parameters;
    protected Map<String, String> replicationProperties;
    protected Map<String, String> endpointAssignmentProperties;

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

    protected boolean isTargetEndpoint(Database db) {
        NetEndpoint localEndpoint = NetEndpoint.getLocalTcpEndpoint();
        return db.isTargetEndpoint(localEndpoint);
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
        String endpointAssignmentStrategy = parameters.get("endpoint_assignment_strategy");

        Collection<String> recognizedReplicationStrategyOptions = NetEndpointManagerHolder.get()
                .getRecognizedReplicationStrategyOptions(replicationStrategy);
        if (recognizedReplicationStrategyOptions == null) {
            recognizedReplicationStrategyOptions = new HashSet<>(1);
        } else {
            recognizedReplicationStrategyOptions = new HashSet<>(recognizedReplicationStrategyOptions);
        }
        recognizedReplicationStrategyOptions.add("replication_strategy");

        Collection<String> recognizedEndpointAssignmentStrategyOptions = NetEndpointManagerHolder.get()
                .getRecognizedEndpointAssignmentStrategyOptions(endpointAssignmentStrategy);

        if (recognizedEndpointAssignmentStrategyOptions == null) {
            recognizedEndpointAssignmentStrategyOptions = new HashSet<>(1);
        } else {
            recognizedEndpointAssignmentStrategyOptions = new HashSet<>(recognizedEndpointAssignmentStrategyOptions);
        }
        recognizedEndpointAssignmentStrategyOptions.add("endpoint_assignment_strategy");

        Collection<String> recognizedSettingOptions = DbSettings.getDefaultSettings().getSettings().keySet();
        parameters.removeAll(recognizedReplicationStrategyOptions);
        parameters.removeAll(recognizedEndpointAssignmentStrategyOptions);
        parameters.removeAll(recognizedSettingOptions);
        if (!parameters.isEmpty()) {
            throw new ConfigException(String.format("Unrecognized parameters: %s for database %s, " //
                    + "recognized replication strategy options: %s, " //
                    + "endpoint assignment strategy options: %s, " //
                    + "database setting options: %s", //
                    parameters.keySet(), dbName, recognizedReplicationStrategyOptions,
                    recognizedEndpointAssignmentStrategyOptions, recognizedSettingOptions));
        }

        parameters = (CaseInsensitiveMap<String>) this.parameters;
        replicationProperties = new CaseInsensitiveMap<>();
        if (!parameters.containsKey("replication_factor")) {
            replicationProperties.put("replication_factor",
                    NetEndpointManagerHolder.get().getDefaultReplicationFactor() + "");
        }
        if (replicationStrategy != null) {
            replicationProperties.put("class", replicationStrategy);
        } else {
            replicationProperties.put("class", NetEndpointManagerHolder.get().getDefaultReplicationStrategy());
        }
        for (String option : recognizedReplicationStrategyOptions) {
            if (parameters.containsKey(option))
                replicationProperties.put(option, parameters.get(option));
        }

        endpointAssignmentProperties = new CaseInsensitiveMap<>();
        if (!parameters.containsKey("assignment_factor")) {
            endpointAssignmentProperties.put("assignment_factor",
                    NetEndpointManagerHolder.get().getDefaultEndpointAssignmentFactor() + "");
        }
        if (endpointAssignmentStrategy != null) {
            endpointAssignmentProperties.put("class", endpointAssignmentStrategy);
        } else {
            endpointAssignmentProperties.put("class",
                    NetEndpointManagerHolder.get().getDefaultEndpointAssignmentStrategy());
        }
        for (String option : recognizedEndpointAssignmentStrategyOptions) {
            if (parameters.containsKey(option))
                endpointAssignmentProperties.put(option, parameters.get(option));
        }
        if (runMode == RunMode.CLIENT_SERVER) {
            endpointAssignmentProperties.put("assignment_factor", "1");
        } else if (runMode == RunMode.REPLICATION) {
            endpointAssignmentProperties.put("assignment_factor", replicationProperties.get("replication_factor"));
        } else if (runMode == RunMode.SHARDING) {
            if (!parameters.containsKey("assignment_factor"))
                endpointAssignmentProperties.put("assignment_factor", replicationProperties.get("replication_factor"));
            // throw new ConfigException("In sharding mode, assignment_factor must be set");
        }
        // parameters剩下的当成database setting
    }
}
