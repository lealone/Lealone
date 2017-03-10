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
package org.lealone.aose.router;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.result.Result;
import org.lealone.sql.StatementBase;
import org.lealone.sql.ddl.DefineStatement;
import org.lealone.sql.dml.TransactionStatement;
import org.lealone.sql.router.Router;

public class TransactionalRouter implements Router {
    private final Router nestedRouter;

    public TransactionalRouter(Router nestedRouter) {
        this.nestedRouter = nestedRouter;
    }

    private void beginTransaction(StatementBase statement) {
        statement.getSession().getTransaction(statement);
    }

    @Override
    public int executeUpdate(StatementBase statement) {
        if (statement instanceof DefineStatement)
            return nestedRouter.executeUpdate(statement);

        beginTransaction(statement);

        boolean isTopTransaction = false;
        boolean isNestedTransaction = false;
        ServerSession session = statement.getSession();

        try {
            if (!statement.isLocal() && statement.isBatch()) {
                if (session.isAutoCommit()) {
                    session.setAutoCommit(false);
                    isTopTransaction = true;
                } else {
                    isNestedTransaction = true;
                    session.addSavepoint(TransactionStatement.INTERNAL_SAVEPOINT);
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
            // default:
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
                session.rollbackToSavepoint(TransactionStatement.INTERNAL_SAVEPOINT);

            throw DbException.convert(e);
        } finally {
            if (isTopTransaction)
                session.setAutoCommit(true);
        }
    }

    @Override
    public Result executeQuery(StatementBase statement, int maxRows) {
        beginTransaction(statement);
        return nestedRouter.executeQuery(statement, maxRows);
    }

    @Override
    public int[] getHostIds(Database db) {
        return nestedRouter.getHostIds(db);
    }

    @Override
    public int createDatabase(Database db, ServerSession currentSession) {
        return nestedRouter.createDatabase(db, currentSession);
    }
}
