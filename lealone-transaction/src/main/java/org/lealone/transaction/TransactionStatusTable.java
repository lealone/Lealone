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

    //    private static PreparedStatement commit;

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

    //    synchronized static void init() {
    //        if (commit != null)
    //            return;
    //        createTableIfNotExists();
    //    }
    //
    //    private static void createTableIfNotExists() {
    //        Statement stmt = null;
    //        Connection conn = SystemDatabase.getConnection();
    //        try {
    //            stmt = conn.createStatement();
    //            stmt.execute("CREATE TABLE IF NOT EXISTS transaction_status_table" //
    //                    + "(transaction_name VARCHAR PRIMARY KEY, all_local_transaction_names VARCHAR, commit_timestamp BIGINT)");
    //
    //            commit = conn.prepareStatement("INSERT INTO transaction_status_table VALUES(?, ?, ?)");
    //        } catch (SQLException e) {
    //            throw DbException.convert(e);
    //        } finally {
    //            JdbcUtils.closeSilently(stmt);
    //        }
    //    }
    //
    //    synchronized static void commit0(GlobalTransaction localTransaction, String allLocalTransactionNames) {
    //        try {
    //            commit.setString(1, localTransaction.getTransactionName());
    //            commit.setString(2, allLocalTransactionNames);
    //            commit.setLong(3, localTransaction.getCommitTimestamp());
    //            commit.executeUpdate();
    //        } catch (SQLException e) {
    //            throw DbException.convert(e);
    //        }
    //    }

    synchronized static void commit(GlobalTransaction localTransaction, String allLocalTransactionNames) {
        Object[] v = { allLocalTransactionNames, localTransaction.getCommitTimestamp() };
        map.put(localTransaction.getTransactionName(), v);
    }

    synchronized static boolean isFullSuccessful(String hostAndPort, long tid) {
        return true;
    }
}
