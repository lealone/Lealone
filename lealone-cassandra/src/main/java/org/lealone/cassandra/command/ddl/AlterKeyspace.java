/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.lealone.cassandra.command.ddl;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.statements.KSPropDefs;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.lealone.cassandra.command.CommandConstants;
import org.lealone.command.ddl.DefineCommand;
import org.lealone.dbobject.User;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

public class AlterKeyspace extends DefineCommand {

    private String keyspaceName;
    private KSPropDefs defs;

    public AlterKeyspace(Session session) {
        super(session);
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public void setKSPropDefs(KSPropDefs defs) {
        this.defs = defs;
    }

    @Override
    public int getType() {
        return CommandConstants.ALTER_KEYSPACE;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        User user = session.getUser();
        user.checkAdmin();

        try {
            KSMetaData ksm = Schema.instance.getKSMetaData(keyspaceName);
            // In the (very) unlikely case the keyspace was dropped since validate()
            if (ksm == null) //validate方法中已检查过了，如果存在一些并发场景，有可能在validate方法到这里之间把ks删除了
                throw new InvalidRequestException("Unknown keyspace " + keyspaceName);

            MigrationManager.announceKeyspaceUpdate(defs.asKSMetadataUpdate(ksm), false);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        return 0;
    }

    public void validate() {
        try {
            KSMetaData ksm = Schema.instance.getKSMetaData(keyspaceName);
            if (ksm == null)
                throw new InvalidRequestException("Unknown keyspace " + keyspaceName);
            if (ksm.name.equalsIgnoreCase(Keyspace.SYSTEM_KS))
                throw new InvalidRequestException("Cannot alter system keyspace");

            if (defs.getReplicationStrategyClass() == null && !defs.getReplicationOptions().isEmpty()) {
                throw new ConfigurationException("Missing replication strategy class");
            } else if (defs.getReplicationStrategyClass() != null) {
                // The strategy is validated through KSMetaData.validate() in announceKeyspaceUpdate below.
                // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
                // so doing proper validation here.
                AbstractReplicationStrategy.validateReplicationStrategy(keyspaceName,
                        AbstractReplicationStrategy.getClass(defs.getReplicationStrategyClass()),
                        StorageService.instance.getTokenMetadata(), DatabaseDescriptor.getEndpointSnitch(),
                        defs.getReplicationOptions());
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }
}
