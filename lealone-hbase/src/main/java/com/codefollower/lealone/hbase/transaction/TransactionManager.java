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
package com.codefollower.lealone.hbase.transaction;

import java.io.IOException;
import com.codefollower.lealone.hbase.tso.client.SyncCreateCallback;
import com.codefollower.lealone.hbase.tso.client.TSOClient;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;

public class TransactionManager {
    public static final TSOClient tsoclient = getTSOClient();

    private static TSOClient getTSOClient() {
        try {
            return new TSOClient(HBaseUtils.getConfiguration());
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Starts a new transaction.
     * 
     * This method returns an opaque {@link Transaction} object, used by
     * {@link TTable}'s methods for performing operations on a given
     * transaction.
     * 
     * @return Opaque object which identifies one transaction.
     * @throws TransactionException
     */
    public static Transaction getNewTransaction() throws TransactionException {
        return new Transaction(getNewTimestamp());
    }

    public static long getNewTimestamp() throws TransactionException {
        SyncCreateCallback cb = new SyncCreateCallback();
        try {
            tsoclient.getNewTimestamp(cb);
            cb.await();
        } catch (Exception e) {
            throw new TransactionException("Could not get new timestamp", e);
        }
        if (cb.getException() != null) {
            throw new TransactionException("Error retrieving timestamp", cb.getException());
        }

        return cb.getStartTimestamp();
    }
}
