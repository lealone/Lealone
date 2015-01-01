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
package org.lealone.transaction;

import org.lealone.command.ddl.DefineCommand;
import org.lealone.command.dml.Delete;
import org.lealone.command.dml.Insert;
import org.lealone.command.dml.Select;
import org.lealone.command.dml.TransactionCommand;
import org.lealone.command.dml.Update;
import org.lealone.command.router.Router;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.ResultInterface;

public class TransactionalRouter implements Router {
    private final Router nestedRouter;

    public TransactionalRouter(Router nestedRouter) {
        this.nestedRouter = nestedRouter;
    }

    @Override
    public int executeDefineCommand(DefineCommand defineCommand) {
        return nestedRouter.executeDefineCommand(defineCommand);
    }

    @Override
    public int executeInsert(Insert insert) {
        boolean isTopTransaction = false;
        boolean isNestedTransaction = false;
        Session session = insert.getSession();
        try {
            if (insert.isBatch()) {
                if (session.getAutoCommit()) {
                    session.setAutoCommit(false);
                    isTopTransaction = true;
                } else {
                    isNestedTransaction = true;
                    session.addSavepoint(TransactionCommand.INTERNAL_SAVEPOINT);
                }
            }
            int updateCount = nestedRouter.executeInsert(insert);
            if (isTopTransaction)
                session.commit(false);
            return updateCount;
        } catch (Exception e) {
            if (isTopTransaction)
                session.rollback();

            //嵌套事务出错时提前rollback
            if (isNestedTransaction)
                session.rollbackToSavepoint(TransactionCommand.INTERNAL_SAVEPOINT);

            throw DbException.convert(e);
        } finally {
            if (isTopTransaction)
                session.setAutoCommit(true);
        }
    }

    @Override
    public int executeDelete(Delete delete) {
        return executeUpdateOrDelete(null, delete);
    }

    @Override
    public int executeUpdate(Update update) {
        return executeUpdateOrDelete(update, null);
    }

    private int executeUpdateOrDelete(Update update, Delete delete) {
        boolean isTopTransaction = false;
        boolean isNestedTransaction = false;

        Session session;
        if (update != null)
            session = update.getSession();
        else
            session = delete.getSession();
        try {
            if (session.getAutoCommit()) {
                session.setAutoCommit(false);
                isTopTransaction = true;
            } else {
                isNestedTransaction = true;
                session.addSavepoint(TransactionCommand.INTERNAL_SAVEPOINT);
            }
            int updateCount;
            if (update != null)
                updateCount = nestedRouter.executeUpdate(update);
            else
                updateCount = nestedRouter.executeDelete(delete);
            if (isTopTransaction)
                session.commit(false);
            return updateCount;
        } catch (Exception e) {
            if (isTopTransaction)
                session.rollback();

            //嵌套事务出错时提前rollback
            if (isNestedTransaction)
                session.rollbackToSavepoint(TransactionCommand.INTERNAL_SAVEPOINT);

            throw DbException.convert(e);
        } finally {
            if (isTopTransaction)
                session.setAutoCommit(true);
        }

    }

    @Override
    public ResultInterface executeSelect(Select select, int maxRows, boolean scrollable) {
        return nestedRouter.executeSelect(select, maxRows, scrollable);
    }

}
