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
package org.lealone.sql.executor.sharding;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.net.NetNode;
import org.lealone.sql.DistributedSQLCommand;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.executor.sharding.result.MergedResult;
import org.lealone.sql.executor.sharding.result.SerializedResult;
import org.lealone.sql.executor.sharding.result.SortedResult;
import org.lealone.sql.query.Select;
import org.lealone.storage.PageKey;

public class ShardingQueryExecutor extends ShardingSqlExecutor {

    public static void executeDistributedQuery(StatementBase statement, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler) {
        beginTransaction(statement);
        Result result = executeDistributedQuery(statement, maxRows, scrollable);
        asyncHandler.handle(new AsyncResult<>(result));
    }

    private static Result executeDistributedQuery(StatementBase statement, int maxRows, boolean scrollable) {
        int type = statement.getType();
        switch (type) {
        case SQLStatement.SELECT: {
            Map<String, List<PageKey>> nodeToPageKeyMap = statement.getNodeToPageKeyMap();
            int size = nodeToPageKeyMap.size();
            if (size <= 0) {
                return new LocalResult();
            }

            ServerSession currentSession = statement.getSession();
            String sql = statement.getPlanSQL(true);
            Session[] sessions = new Session[size];
            DistributedSQLCommand[] commands = new DistributedSQLCommand[size];
            ArrayList<Callable<Result>> callables = new ArrayList<>(size);
            int i = 0;
            for (Entry<String, List<PageKey>> e : nodeToPageKeyMap.entrySet()) {
                String hostId = e.getKey();
                List<PageKey> pageKeys = e.getValue();
                sessions[i] = currentSession.getNestedSession(hostId,
                        !NetNode.getLocalTcpNode().equals(NetNode.createTCP(hostId)));
                commands[i] = sessions[i].createDistributedSQLCommand(sql, Integer.MAX_VALUE);
                DistributedSQLCommand c = commands[i];
                callables.add(() -> {
                    return c.executeDistributedQuery(maxRows, scrollable, pageKeys).get();
                });
                i++;
            }

            try {
                Select select = (Select) statement;
                if (!select.isGroupQuery() && select.getSortOrder() == null) {
                    return new SerializedResult(callables, maxRows, scrollable, select.getLimitRows());
                } else {
                    ArrayList<Future<Result>> futures = new ArrayList<>(size);
                    ArrayList<Result> results = new ArrayList<>(size);
                    for (Callable<Result> callable : callables) {
                        futures.add(executorService.submit(callable));
                    }

                    for (Future<Result> f : futures) {
                        results.add(f.get());
                    }

                    if (!select.isGroupQuery() && select.getSortOrder() != null)
                        return new SortedResult(maxRows, select.getSession(), select, results);

                    String newSQL = select.getPlanSQL(true, true);
                    Select newSelect = (Select) select.getSession().prepareStatement(newSQL, true);
                    newSelect.setLocal(true);

                    return new MergedResult(results, newSelect, select);
                }
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
        default:
            return statement.query(maxRows);
        }
    }
}
