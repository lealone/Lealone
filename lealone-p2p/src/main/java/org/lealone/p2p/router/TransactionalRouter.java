/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownershistatement.  The ASF licenses this file
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
package org.lealone.p2p.router;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.db.result.Result;
import org.lealone.net.NetEndpoint;
import org.lealone.replication.ReplicationSession;
import org.lealone.router.Router;
import org.lealone.sql.PreparedStatement;
import org.lealone.sql.SQLStatement;

public class TransactionalRouter implements Router {
    private final Router nestedRouter;

    public TransactionalRouter(Router nestedRouter) {
        this.nestedRouter = nestedRouter;
    }

    private void beginTransaction(PreparedStatement statement) {
        statement.getSession().getTransaction(statement);
    }

    @Override
    public int executeUpdate(PreparedStatement statement) {
        if (statement.isDDL())
            return nestedRouter.executeUpdate(statement);

        beginTransaction(statement);

        boolean isTopTransaction = false;
        boolean isNestedTransaction = false;
        Session session = statement.getSession();

        try {
            if (!statement.isLocal() && statement.isBatch()) {
                if (session.isAutoCommit()) {
                    session.setAutoCommit(false);
                    isTopTransaction = true;
                } else {
                    isNestedTransaction = true;
                    session.addSavepoint(SQLStatement.INTERNAL_SAVEPOINT);
                }
            }

            // int updateCount = 0;
            // switch (statement.getType()) {
            // case SQLStatement.INSERT:
            // case SQLStatement.UPDATE:
            // case SQLStatement.DELETE:
            // case SQLStatement.MERGE:
            // updateCount = nestedRouter.executeUpdate(statement);
            // break;
            // :
            // }

            int updateCount = nestedRouter.executeUpdate(statement);
            if (isTopTransaction)
                session.prepareCommit();
            return updateCount;
        } catch (Exception e) {
            if (isTopTransaction)
                session.rollback();

            // 嵌套事务出错时提前rollback
            if (isNestedTransaction)
                session.rollbackToSavepoint(SQLStatement.INTERNAL_SAVEPOINT);

            throw DbException.convert(e);
        } finally {
            if (isTopTransaction)
                session.setAutoCommit(true);
        }
    }

    @Override
    public Result executeQuery(PreparedStatement statement, int maxRows) {
        beginTransaction(statement);
        return nestedRouter.executeQuery(statement, maxRows);
    }

    @Override
    public String[] assignEndpoints(IDatabase db) {
        return nestedRouter.assignEndpoints(db);
    }

    @Override
    public int executeDatabaseStatement(IDatabase db, Session currentSession, PreparedStatement statement) {
        return nestedRouter.executeDatabaseStatement(db, currentSession, statement);
    }

    @Override
    public void replicate(IDatabase db, RunMode oldRunMode, RunMode newRunMode, String[] newReplicationEndpoints) {
        nestedRouter.replicate(db, oldRunMode, newRunMode, newReplicationEndpoints);
    }

    @Override
    public String[] getReplicationEndpoints(IDatabase db) {
        return nestedRouter.getReplicationEndpoints(db);
    }

    @Override
    public void sharding(IDatabase db, RunMode oldRunMode, RunMode newRunMode, String[] oldEndpoints,
            String[] newEndpoints) {
        nestedRouter.sharding(db, oldRunMode, newRunMode, oldEndpoints, newEndpoints);
    }

    @Override
    public String[] getShardingEndpoints(IDatabase db) {
        return nestedRouter.getShardingEndpoints(db);
    }

    @Override
    public void scaleIn(IDatabase db, RunMode oldRunMode, RunMode newRunMode, String[] oldEndpoints,
            String[] newEndpoints) {
        nestedRouter.scaleIn(db, oldRunMode, newRunMode, oldEndpoints, newEndpoints);
    }

    @Override
    public ReplicationSession createReplicationSession(Session session, Collection<NetEndpoint> replicationEndpoints) {
        return nestedRouter.createReplicationSession(session, replicationEndpoints);
    }

    @Override
    public ReplicationSession createReplicationSession(Session session, Collection<NetEndpoint> replicationEndpoints,
            Boolean remote) {
        return nestedRouter.createReplicationSession(session, replicationEndpoints, remote);
    }

    @Override
    public NetEndpoint getEndpoint(String hostId) {
        return nestedRouter.getEndpoint(hostId);
    }

    @Override
    public String getHostId(NetEndpoint endpoint) {
        return nestedRouter.getHostId(endpoint);
    }

    @Override
    public List<NetEndpoint> getReplicationEndpoints(IDatabase db, Set<NetEndpoint> oldReplicationEndpoints,
            Set<NetEndpoint> candidateEndpoints) {
        return nestedRouter.getReplicationEndpoints(db, oldReplicationEndpoints, candidateEndpoints);
    }

}
