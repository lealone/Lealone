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
package com.codefollower.lealone.atomicdb.cql.statements;

import com.codefollower.lealone.atomicdb.auth.Permission;
import com.codefollower.lealone.atomicdb.cql.CFName;
import com.codefollower.lealone.atomicdb.exceptions.ConfigurationException;
import com.codefollower.lealone.atomicdb.exceptions.InvalidRequestException;
import com.codefollower.lealone.atomicdb.exceptions.UnauthorizedException;
import com.codefollower.lealone.atomicdb.service.ClientState;
import com.codefollower.lealone.atomicdb.service.MigrationManager;
import com.codefollower.lealone.atomicdb.transport.messages.ResultMessage;

public class DropTableStatement extends SchemaAlteringStatement
{
    private final boolean ifExists;

    public DropTableStatement(CFName name, boolean ifExists)
    {
        super(name);
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.DROP);
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    public void announceMigration() throws ConfigurationException
    {
        try
        {
            MigrationManager.announceColumnFamilyDrop(keyspace(), columnFamily());
        }
        catch (ConfigurationException e)
        {
            if (!ifExists)
                throw e;
        }
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.DROPPED;
    }
}
