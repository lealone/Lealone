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

import org.lealone.db.RunMode;
import org.lealone.net.NetNode;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;

public class AOTransactionEngine extends AMTransactionEngine {

    private static final String NAME = "AOTE";

    public AOTransactionEngine() {
        super(NAME);
    }

    @Override
    protected AMTransaction createTransaction(long tid, RunMode runMode) {
        if (runMode == RunMode.REPLICATION || runMode == RunMode.SHARDING)
            return new AOTransaction(this, tid);
        else
            return new AMTransaction(this, tid);
    }

    @Override
    public boolean validateTransaction(String localTransactionName) {
        return DTRValidator.validateTransaction(localTransactionName);
    }

    boolean validateTransaction(long tid, AOTransaction currentTransaction) {
        return DTRValidator.validateTransaction(NetNode.getLocalTcpHostAndPort(), tid, currentTransaction);
    }

    @Override
    protected TransactionMap<?, ?> getTransactionMap(Transaction transaction,
            StorageMap<Object, TransactionalValue> map) {
        return new AOTransactionMap<>((AOTransaction) transaction, map);
    }
}
