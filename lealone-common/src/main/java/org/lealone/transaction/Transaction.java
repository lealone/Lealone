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

import org.lealone.type.DataType;

public interface Transaction {

    /**
     * The status of a closed transaction (committed or rolled back).
     */
    public static final int STATUS_CLOSED = 0;

    /**
     * The status of an open transaction.
     */
    public static final int STATUS_OPEN = 1;

    /**
     * The status of a prepared transaction.
     */
    public static final int STATUS_PREPARED = 2;

    /**
     * The status of a transaction that is being committed, but possibly not
     * yet finished. A transactions can go into this state when the store is
     * closed while the transaction is committing. When opening a store,
     * such transactions should be committed.
     */
    public static final int STATUS_COMMITTING = 3;

    String getName(); //用于2pc

    void setName(String name); //用于2pc

    void prepare(); //用于2pc

    int getStatus();

    void setStatus(int status);

    //long getTransactionId();

    //long getCommitTimestamp();

    boolean isAutoCommit();

    void setAutoCommit(boolean autoCommit);

    void setLocal(boolean local);

    void addLocalTransactionNames(String localTransactionNames);

    String getLocalTransactionNames();

    void setValidator(Validator validator);

    void addParticipant(Participant participant);

    <K, V> TransactionMap<K, V> openMap(String name);

    <K, V> TransactionMap<K, V> openMap(String name, DataType keyType, DataType valueType);

    void addSavepoint(String name);

    long getSavepointId();

    void commit();

    void commit(String allLocalTransactionNames);

    void rollback();

    void rollbackToSavepoint(String name);

    void rollbackToSavepoint(long savepointId);

    interface Participant {
        void addSavepoint(String name);

        void rollbackToSavepoint(String name);

        void commitTransaction(String localTransactionName);

        void rollbackTransaction();
    }

    interface Validator {
        boolean validateTransaction(String localTransactionName);
    }
}
