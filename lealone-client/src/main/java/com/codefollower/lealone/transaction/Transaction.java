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

import java.io.IOException;

import com.codefollower.lealone.value.Transfer;

public interface Transaction {
    public void setTransactionId(long transactionId);

    public long getTransactionId();

    public void setCommitTimestamp(long commitTimestamp);

    public long getCommitTimestamp();

    public void setAutoCommit(boolean autoCommit);

    public boolean isAutoCommit();

    public void addCommitInfo(CommitInfo commitInfo);

    public CommitInfo[] getAllCommitInfo();

    public void releaseResources();

    public class CommitInfo {
        private final String hostAndPort;
        private final long[] transactionIds;
        private final long[] commitTimestamps;

        public CommitInfo(String hostAndPort, long[] transactionIds, long[] commitTimestamps) {
            this.hostAndPort = hostAndPort;
            this.transactionIds = transactionIds;
            this.commitTimestamps = commitTimestamps;
        }

        public String getHostAndPort() {
            return hostAndPort;
        }

        public long[] getTransactionIds() {
            return transactionIds;
        }

        public long[] getCommitTimestamps() {
            return commitTimestamps;
        }

        public String[] getKeys() {
            int len = transactionIds.length;
            StringBuilder buff = new StringBuilder(hostAndPort);
            buff.append(':');
            int buffLenMark = buff.length();
            String[] keys = new String[len];
            for (int i = 0; i < len; i++) {
                keys[i] = buff.append(transactionIds[i]).toString();
                buff.setLength(buffLenMark);
            }

            return keys;
        }

        public String getKey(long tid) {
            return getKey(hostAndPort, tid);
        }

        public void write(Transfer transfer) throws IOException {
            transfer.writeString(hostAndPort);
            int len = transactionIds.length;
            transfer.writeInt(len);
            for (int i = 0; i < len; i++) {
                transfer.writeLong(transactionIds[i]);
                transfer.writeLong(commitTimestamps[i]);
            }
        }

        public static CommitInfo read(Transfer transfer) throws IOException {
            String hostAndPort = transfer.readString();
            int len = transfer.readInt();
            long[] transactionIds = new long[len];
            long[] commitTimestamps = new long[len];
            for (int i = 0; i < len; i++) {
                transactionIds[i] = transfer.readLong();
                commitTimestamps[i] = transfer.readLong();
            }

            return new CommitInfo(hostAndPort, transactionIds, commitTimestamps);
        }

        public static String getKey(String hostAndPort, long tid) {
            StringBuilder buff = new StringBuilder(hostAndPort);
            buff.append(':');
            buff.append(tid);
            return buff.toString();
        }
    }
}
