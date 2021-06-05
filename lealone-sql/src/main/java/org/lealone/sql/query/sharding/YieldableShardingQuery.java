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
package org.lealone.sql.query.sharding;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionStatus;
import org.lealone.net.NetNode;
import org.lealone.sql.DistributedSQLCommand;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.query.Select;
import org.lealone.sql.query.YieldableQueryBase;
import org.lealone.storage.PageKey;

public class YieldableShardingQuery extends YieldableQueryBase {

    private SQOperator queryOperator;

    public YieldableShardingQuery(StatementBase statement, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler) {
        super(statement, maxRows, scrollable, asyncHandler);
    }

    @Override
    protected boolean startInternal() {
        statement.getSession().getTransaction(statement);
        queryOperator = createShardingQueryOperator();
        queryOperator.setSession(session);
        queryOperator.start();
        return false;
    }

    @Override
    protected void executeInternal() {
        if (!queryOperator.end) {
            session.setStatus(SessionStatus.STATEMENT_RUNNING);
            queryOperator.run();
        }
        if (queryOperator.end) {
            if (queryOperator.pendingException != null) {
                setPendingException(queryOperator.pendingException);
            } else {
                setResult(queryOperator.result, queryOperator.result.getRowCount());
            }
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        }
    }

    private SQOperator createShardingQueryOperator() {
        switch (statement.getType()) {
        case SQLStatement.SELECT: {
            Map<String, List<PageKey>> nodeToPageKeyMap = statement.getNodeToPageKeyMap();
            int size = nodeToPageKeyMap.size();
            if (size <= 0) {
                return new SQEmpty();
            }
            SQCommand[] commands = createCommands(nodeToPageKeyMap);
            Select select = (Select) statement;
            if (select.isGroupQuery()) {
                return new SQMerge(commands, maxRows, select);
            } else {
                if (select.getSortOrder() != null) {
                    return new SQSort(commands, maxRows, select);
                } else {
                    return new SQSerialize(commands, maxRows, select.getLimitRows());
                }
            }
        }
        default:
            return new SQDirect(statement, maxRows);
        }
    }

    private SQCommand[] createCommands(Map<String, List<PageKey>> nodeToPageKeyMap) {
        ServerSession currentSession = statement.getSession();
        String sql = statement.getPlanSQL(true);
        SQCommand[] commands = new SQCommand[nodeToPageKeyMap.size()];
        int i = 0;
        for (Entry<String, List<PageKey>> e : nodeToPageKeyMap.entrySet()) {
            String hostId = e.getKey();
            List<PageKey> pageKeys = e.getValue();
            Session s = currentSession.getNestedSession(hostId, !NetNode.isLocalTcpNode(hostId));
            DistributedSQLCommand c = s.createDistributedSQLCommand(sql, Integer.MAX_VALUE);
            commands[i] = new SQCommand(c, maxRows, scrollable, pageKeys);
            i++;
        }
        return commands;
    }
}
