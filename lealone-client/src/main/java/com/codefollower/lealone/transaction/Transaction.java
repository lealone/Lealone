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
package com.codefollower.lealone.transaction;

import java.util.Set;

import com.codefollower.lealone.util.New;

public class Transaction {

    private final Set<Transaction> children = New.hashSet();

    private long transactionId;
    private long commitTimestamp;
    private String hostAndPort;
    private boolean autoCommit = true;

    public Set<Transaction> getChildren() {
        return children;
    }

    public void addChildren(Set<Transaction> dts) {
        children.addAll(dts);
    }

    public void addChild(Transaction dt) {
        children.add(dt);
    }

    public long getStartTimestamp() {
        return transactionId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public String getHostAndPort() {
        return hostAndPort;
    }

    public void setHostAndPort(String hostAndPort) {
        this.hostAndPort = hostAndPort;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hostAndPort == null) ? 0 : hostAndPort.hashCode());
        result = prime * result + (int) (transactionId ^ (transactionId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Transaction other = (Transaction) obj;
        if (hostAndPort == null) {
            if (other.hostAndPort != null)
                return false;
        } else if (!hostAndPort.equals(other.hostAndPort))
            return false;
        if (transactionId != other.transactionId)
            return false;
        return true;
    }

    public String toString() {
        return "T-" + transactionId;
    }

}
