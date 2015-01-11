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

import org.lealone.mvstore.MVMap;
import org.lealone.mvstore.MVStore;

class TransactionStatusTable {
    private TransactionStatusTable() {
    }

    /**
     * The persisted map of transactionStatusTable.
     * Key: transaction_name, value: [ all_local_transaction_names, commit_timestamp ].
     */
    private static MVMap<String, Object[]> map;

    synchronized static void init(MVStore store) {
        if (map != null)
            return;
        map = store.openMap("transactionStatusTable", new MVMap.Builder<String, Object[]>());
    }

    synchronized static void commit(GlobalTransaction localTransaction, String allLocalTransactionNames) {
        Object[] v = { allLocalTransactionNames, localTransaction.getCommitTimestamp() };
        map.put(localTransaction.getTransactionName(), v);
    }

    synchronized static boolean isFullSuccessful(String hostAndPort, long tid) {
        return true;
    }
}
