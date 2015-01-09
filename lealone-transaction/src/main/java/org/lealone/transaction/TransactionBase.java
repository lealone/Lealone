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

import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.engine.Session;
import org.lealone.result.Row;

public class TransactionBase implements Transaction {
    protected final Session session;
    //protected final String transactionName;
    protected final long transactionId;
    protected final boolean autoCommit;
    protected long commitTimestamp;
    protected CopyOnWriteArrayList<Row> undoRows;
    protected HashMap<String, Integer> savepoints;

    public TransactionBase(Session session) {
        session.setTransaction(this);
        this.session = session;
        undoRows = new CopyOnWriteArrayList<>();
        autoCommit = session.getAutoCommit();

        transactionId = getNewTimestamp();
        //        String hostAndPort = session.getHostAndPort();
        //        if (hostAndPort == null)
        //            hostAndPort = "localhost:0";
        //transactionName = getTransactionName(hostAndPort, transactionId);
    }

    private long getNewTimestamp() {
        if (autoCommit)
            return TimestampServiceTable.nextEven();
        else
            return TimestampServiceTable.nextOdd();
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public long getCommitTimestamp() {
        return 0;
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void addLocalTransactionNames(String localTransactionNames) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getLocalTransactionNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void commit() {
        // TODO Auto-generated method stub

    }

    @Override
    public void commit(String allLocalTransactionNames) {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback() {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollbackToSavepoint(String name) {
        // TODO Auto-generated method stub

    }

}
