/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.lealone.mvstore.engine;

import com.codefollower.lealone.dbobject.User;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.mvstore.dbobject.TransactionStore;
import com.codefollower.lealone.mvstore.dbobject.TransactionStore.Transaction;
import com.codefollower.lealone.result.Row;

public class MVSession extends Session {

    private Transaction transaction;
    private long startStatement = -1;

    public MVSession(Database database, User user, int id) {
        super(database, user, id);
    }

    @Override
    public void log(Table table, short operation, Row row) {
        if (table.isMVStore()) {
            return;
        }
        super.log(table, operation, row);
    }

    @Override
    public void begin() {
        super.begin();
        setAutoCommit(false);
    }

    @Override
    public void commit(boolean ddl) {
        if (transaction != null) {
            transaction.commit();
            transaction = null;
        }
        super.commit(ddl);
    }

    @Override
    public void rollback() {
        if (transaction != null) {
            transaction.rollback();
            transaction = null;
        }
        super.rollback();
    }

    /**
     * Get the transaction to use for this session.
     *
     * @param store the store
     * @return the transaction
     */
    public Transaction getTransaction(TransactionStore store) {
        if (transaction == null) {
            transaction = store.begin();
            startStatement = -1;
        }
        return transaction;
    }

    public long getStatementSavepoint() {
        if (startStatement == -1) {
            startStatement = transaction.setSavepoint();
        }
        return startStatement;
    }
}
