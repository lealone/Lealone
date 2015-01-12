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
package org.lealone.cassandra.command.ddl;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.MigrationManager;
import org.lealone.cassandra.command.CommandConstants;
import org.lealone.command.ddl.DefineCommand;
import org.lealone.dbobject.User;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

public class DropKeyspace extends DefineCommand {

    private String keyspaceName;
    private boolean ifExists;

    public DropKeyspace(Session session) {
        super(session);
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    @Override
    public int getType() {
        return CommandConstants.DROP_KEYSPACE;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        User user = session.getUser();
        user.checkAdmin();

        try {
            MigrationManager.announceKeyspaceDrop(keyspaceName, false);
        } catch (ConfigurationException e) {
            if (!ifExists)
                throw DbException.convert(e);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        return 0;
    }
}
