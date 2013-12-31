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

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codefollower.lealone.atomicdb.auth.Permission;
import com.codefollower.lealone.atomicdb.config.CFMetaData;
import com.codefollower.lealone.atomicdb.config.ColumnDefinition;
import com.codefollower.lealone.atomicdb.config.IndexType;
import com.codefollower.lealone.atomicdb.config.Schema;
import com.codefollower.lealone.atomicdb.cql.*;
import com.codefollower.lealone.atomicdb.db.index.SecondaryIndex;
import com.codefollower.lealone.atomicdb.exceptions.*;
import com.codefollower.lealone.atomicdb.service.ClientState;
import com.codefollower.lealone.atomicdb.service.MigrationManager;
import com.codefollower.lealone.atomicdb.transport.messages.ResultMessage;
import com.google.common.collect.ImmutableMap;


/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
public class CreateIndexStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);

    private final String indexName;
    private final ColumnIdentifier columnName;
    private final boolean ifNotExists;
    private final boolean isCustom;
    private final String indexClass;

    public CreateIndexStatement(CFName name, String indexName, ColumnIdentifier columnName, boolean ifNotExists, boolean isCustom, String indexClass)
    {
        super(name);
        this.indexName = indexName;
        this.columnName = columnName;
        this.ifNotExists = ifNotExists;
        this.isCustom = isCustom;
        this.indexClass = indexClass;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        CFMetaData cfm = OldThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        if (cfm.getDefaultValidator().isCommutative())
            throw new InvalidRequestException("Secondary indexes are not supported on counter tables");

        ColumnDefinition cd = cfm.getColumnDefinition(columnName);

        if (cd == null)
            throw new InvalidRequestException("No column definition found for column " + columnName);

        if (cd.getIndexType() != null)
        {
            if (ifNotExists)
                return;
            else
                throw new InvalidRequestException("Index already exists");
        }

        if (isCustom && indexClass == null)
            throw new InvalidRequestException("CUSTOM index requires specifiying the index class");

        if (!isCustom && indexClass != null)
            throw new InvalidRequestException("Cannot specify index class for a non-CUSTOM index");

        // TODO: we could lift that limitation
        if (cfm.comparator.isDense() && cd.kind != ColumnDefinition.Kind.REGULAR)
            throw new InvalidRequestException(String.format("Secondary index on %s column %s is not yet supported for compact table", cd.kind, columnName));

        if (cd.kind == ColumnDefinition.Kind.PARTITION_KEY && cd.isOnAllComponents())
            throw new InvalidRequestException(String.format("Cannot add secondary index to already primarily indexed column %s", columnName));
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        logger.debug("Updating column {} definition for index {}", columnName, indexName);
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).clone();
        ColumnDefinition cd = cfm.getColumnDefinition(columnName);

        if (cd.getIndexType() != null && ifNotExists)
            return;

        if (isCustom)
        {
            cd.setIndexType(IndexType.CUSTOM, Collections.singletonMap(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, indexClass));
        }
        else if (cfm.comparator.isCompound())
        {
            Map<String, String> options = Collections.emptyMap();
            // For now, we only allow indexing values for collections, but we could later allow
            // to also index map keys, so we record that this is the values we index to make our
            // lives easier then.
            if (cd.type.isCollection())
                options = ImmutableMap.of("index_values", "");
            cd.setIndexType(IndexType.COMPOSITES, Collections.<String, String>emptyMap());
        }
        else
        {
            cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());
        }

        cd.setIndexName(indexName);
        cfm.addDefaultIndexNames();
        MigrationManager.announceColumnFamilyUpdate(cfm, false);
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        // Creating an index is akin to updating the CF
        return ResultMessage.SchemaChange.Change.UPDATED;
    }
}
