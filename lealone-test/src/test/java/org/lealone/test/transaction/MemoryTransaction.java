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
package org.lealone.test.transaction;

import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;
import org.lealone.type.DataType;

//TODO 实现所有API
public class MemoryTransaction implements Transaction {

    @Override
    public String getName() {

        return null;
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public void prepare() {

    }

    @Override
    public int getStatus() {

        return 0;
    }

    @Override
    public void setStatus(int status) {

    }

    @Override
    public boolean isAutoCommit() {

        return false;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {

    }

    @Override
    public void setLocal(boolean local) {

    }

    @Override
    public void addLocalTransactionNames(String localTransactionNames) {

    }

    @Override
    public String getLocalTransactionNames() {

        return null;
    }

    @Override
    public void setValidator(Validator validator) {

    }

    @Override
    public void addParticipant(Participant participant) {

    }

    @Override
    public <K, V> TransactionMap<K, V> openMap(String name) {

        return null;
    }

    @Override
    public <K, V> TransactionMap<K, V> openMap(String name, DataType keyType, DataType valueType) {
        return new MemoryTransactionMap<>();
    }

    @Override
    public void addSavepoint(String name) {

    }

    @Override
    public long getSavepointId() {

        return 0;
    }

    @Override
    public void commit() {

    }

    @Override
    public void commit(String allLocalTransactionNames) {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void rollbackToSavepoint(String name) {

    }

    @Override
    public void rollbackToSavepoint(long savepointId) {

    }

}
