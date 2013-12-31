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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codefollower.lealone.atomicdb.config.CFMetaData;
import com.codefollower.lealone.atomicdb.config.Schema;
import com.codefollower.lealone.atomicdb.cql.CFName;
import com.codefollower.lealone.atomicdb.cql.OldThriftValidation;
import com.codefollower.lealone.atomicdb.exceptions.ConfigurationException;
import com.codefollower.lealone.atomicdb.exceptions.RequestValidationException;
import com.codefollower.lealone.atomicdb.exceptions.UnauthorizedException;
import com.codefollower.lealone.atomicdb.service.ClientState;
import com.codefollower.lealone.atomicdb.service.MigrationManager;
import com.codefollower.lealone.atomicdb.transport.messages.ResultMessage;

public class DropTriggerStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(DropTriggerStatement.class);

    private final String triggerName;

    public DropTriggerStatement(CFName name, String triggerName)
    {
        super(name);
        this.triggerName = triggerName;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        state.ensureIsSuper("Only superusers are allowed to perfrom DROP TRIGGER queries");
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        OldThriftValidation.validateColumnFamily(keyspace(), columnFamily());
    }

    public void announceMigration() throws ConfigurationException
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).clone();
        if (!cfm.removeTrigger(triggerName))
            throw new ConfigurationException(String.format("Trigger %s was not found", triggerName));
        logger.info("Dropping trigger with name {}", triggerName);
        MigrationManager.announceColumnFamilyUpdate(cfm, false);
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.UPDATED;
    }
}
