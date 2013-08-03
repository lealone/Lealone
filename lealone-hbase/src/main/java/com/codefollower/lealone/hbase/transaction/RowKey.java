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

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MurmurHash;

public class RowKey {
    private final byte[] rowId;
    private final byte[] tableId;
    private int hash = 0;

    public RowKey(byte[] r, byte[] t) {
        rowId = r;
        tableId = t;
    }

    @Override
    public String toString() {
        return Bytes.toString(tableId) + ":" + Bytes.toString(rowId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RowKey) {
            RowKey other = (RowKey) obj;

            return Bytes.equals(other.rowId, rowId) && Bytes.equals(other.tableId, tableId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        byte[] key = Arrays.copyOf(tableId, tableId.length + rowId.length);
        System.arraycopy(rowId, 0, key, tableId.length, rowId.length);
        hash = MurmurHash.getInstance().hash(key, 0, key.length, 0xdeadbeef);
        return hash;
    }
}
