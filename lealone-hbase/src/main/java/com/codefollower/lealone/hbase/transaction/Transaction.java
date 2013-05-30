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

import java.util.HashSet;
import java.util.Set;

public class Transaction {
    private final Set<RowKey> rows;
    private final long startTimestamp;
    private long commitTimestamp;

    Transaction(long startTimestamp) {
        this.rows = new HashSet<RowKey>();
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = 0;
    }

    public long getTransactionId() {
        return startTimestamp;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public RowKey[] getRows() {
        return rows.toArray(new RowKey[0]);
    }

    public void addRow(RowKey row) {
        rows.add(row);
    }

    public String toString() {
        return "Transaction-" + startTimestamp;
    }
}
