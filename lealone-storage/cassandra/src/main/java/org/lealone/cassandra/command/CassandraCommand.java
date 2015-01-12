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
package org.lealone.cassandra.command;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.lealone.api.ParameterInterface;
import org.lealone.command.Command;
import org.lealone.command.Prepared;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.ResultInterface;

public class CassandraCommand extends Command {
    private QueryOptions options;
    private CQLStatement statement;
    private QueryState queryState;

    private CassandraPrepared prepared;

    public CassandraCommand(Session session, String sql, ResultMessage.Prepared prepared, ClientState clientState) {
        super(session, sql);
        options = QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, Collections.<ByteBuffer> emptyList());
        ParsedStatement.Prepared p = ClientState.getCQLQueryHandler().getPrepared(prepared.statementId);
        statement = p.statement;
        queryState = new QueryState(clientState);

        this.prepared = new CassandraPrepared(session);
    }

    @Override
    public int getCommandType() {
        return 0;
    }

    @Override
    public boolean isTransactional() {
        return false;
    }

    @Override
    public boolean isQuery() {
        return statement instanceof SelectStatement;
    }

    static ArrayList<? extends ParameterInterface> emptyList = new ArrayList<ParameterInterface>(0);

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return emptyList; //statement.getBoundTerms();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public Prepared getPrepared() {
        return prepared;
    }

    @Override
    public int update() {
        try {
            ClientState.getCQLQueryHandler().processPrepared(statement, queryState, options);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        return 0;
    }

    @Override
    public ResultInterface query(int maxrows) {
        try {
            ResultMessage result = ClientState.getCQLQueryHandler().processPrepared(statement, queryState, options);
            if (result instanceof ResultMessage.Rows) {
                ResultMessage.Rows rows = (ResultMessage.Rows) result;
                return new CassandraResult(rows.result);
            } else {
                throw DbException.throwInternalError("query: " + sql);
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }
}
