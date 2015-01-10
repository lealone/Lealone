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

import org.lealone.command.CommandInterface;
import org.lealone.command.Prepared;
import org.lealone.command.ddl.DefineCommand;
import org.lealone.command.dml.Delete;
import org.lealone.command.dml.Insert;
import org.lealone.command.dml.Merge;
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
        beginTransaction(defineCommand);
        return nestedRouter.executeDefineCommand(defineCommand);
    }

    @Override
    public int executeInsert(Insert insert) {
        insert.getTable().getTransaction(insert.getSession());
        return execute(insert.isBatch(), insert);
    }

    @Override
    public int executeMerge(Merge merge) {
        merge.getTable().getTransaction(merge.getSession());
        return execute(merge.isBatch(), merge);
    }

    @Override
    public int executeDelete(Delete delete) {
        delete.getTable().getTransaction(delete.getSession());
        return execute(true, delete);
    }

    @Override
    public int executeUpdate(Update update) {
        update.getTable().getTransaction(update.getSession());
        return execute(true, update);
    }

    @Override
    public ResultInterface executeSelect(Select select, int maxRows, boolean scrollable) {
        select.getTable().getTransaction(select.getSession());
        beginTransaction(select);
        return nestedRouter.executeSelect(select, maxRows, scrollable);
    }

    private void beginTransaction(Prepared p) {
        //        if (p.getSession().getTransaction() == null)
        //            TransactionManager.beginTransaction(p.getSession());
    }

    private int execute(boolean isBatch, Prepared p) {
        beginTransaction(p);

        boolean isTopTransaction = false;
        boolean isNestedTransaction = false;
        Session session = p.getSession();

        try {
            if (isBatch) {
                if (session.getAutoCommit()) {
                    session.setAutoCommit(false);
                    isTopTransaction = true;
                } else {
                    isNestedTransaction = true;
                    session.addSavepoint(TransactionCommand.INTERNAL_SAVEPOINT);
                }
            }
            int updateCount = 0;
            switch (p.getType()) {
            case CommandInterface.INSERT:
                updateCount = nestedRouter.executeInsert((Insert) p);
                break;
            case CommandInterface.UPDATE:
                updateCount = nestedRouter.executeUpdate((Update) p);
                break;
            case CommandInterface.DELETE:
                updateCount = nestedRouter.executeDelete((Delete) p);
                break;
            case CommandInterface.MERGE:
                updateCount = nestedRouter.executeMerge((Merge) p);
                break;
            }
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
}
