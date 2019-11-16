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
package org.lealone.transaction.aote;

import java.util.Map;

import org.lealone.net.NetNode;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.AMTransactionEngine;
import org.lealone.transaction.aote.TransactionalValue;

public class AOTransactionEngine extends AMTransactionEngine {

    private static final String NAME = "AOTE";

    public AOTransactionEngine() {
        super(NAME);
    }

    @Override
    public synchronized void init(Map<String, String> config) {
        super.init(config);
        TransactionValidator.getInstance().start();
    }

    @Override
    public void close() {
        super.close();
        TransactionValidator.getInstance().close();
    }

    @Override
    protected AOTransaction createTransaction(long tid) {
        return new AOTransaction(this, tid);
    }

    @Override
    public boolean validateTransaction(String localTransactionName) {
        return TransactionStatusTable.validateTransaction(localTransactionName);
    }

    boolean validateTransaction(long tid, AOTransaction currentTransaction) {
        return TransactionStatusTable.validateTransaction(NetNode.getLocalTcpHostAndPort(), tid, currentTransaction);
    }

    @Override
    protected TransactionMap<?, ?> getTransactionMap(Transaction transaction,
            StorageMap<Object, TransactionalValue> map) {
        return new AOTransactionMap<>((AOTransaction) transaction, map);
    }
}
