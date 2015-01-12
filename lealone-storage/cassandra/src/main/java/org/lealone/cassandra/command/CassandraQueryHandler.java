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

import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Prepared;
import org.apache.cassandra.utils.MD5Digest;

public class CassandraQueryHandler implements QueryHandler {

    @Override
    public ResultMessage process(String query, QueryState state, QueryOptions options) throws RequestExecutionException,
            RequestValidationException {
        return QueryProcessor.instance.process(query, state, options);
    }

    @Override
    public Prepared prepare(String query, QueryState state) throws RequestValidationException {
        return QueryProcessor.instance.prepare(query, state);
    }

    @Override
    public org.apache.cassandra.cql3.statements.ParsedStatement.Prepared getPrepared(MD5Digest id) {
        return QueryProcessor.instance.getPrepared(id);
    }

    @Override
    public org.apache.cassandra.cql3.statements.ParsedStatement.Prepared getPreparedForThrift(Integer id) {
        return QueryProcessor.instance.getPreparedForThrift(id);
    }

    @Override
    public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options)
            throws RequestExecutionException, RequestValidationException {
        return QueryProcessor.instance.processPrepared(statement, state, options);
    }

    @Override
    public ResultMessage processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options)
            throws RequestExecutionException, RequestValidationException {
        return QueryProcessor.instance.processBatch(statement, state, options);
    }

}
