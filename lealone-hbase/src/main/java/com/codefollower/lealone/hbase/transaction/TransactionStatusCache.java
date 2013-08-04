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

/**
 * 
 * 事务状态缓存，用于提高查询性能，有三种事务状态: 
 * <ul>-1: 事务状态未知</ul>
 * <ul>-2: 事务未正常提交</ul>
 * <ul>其他值: 事务正常提交</ul>
 *
 */
public class TransactionStatusCache {
    private static final int BUCKET_NUMBER = 1 << 15; //桶个数
    private static final int BUCKET_SIZE = 1 << 14; //每个桶的容量大小

    private final Bucket buckets[] = new Bucket[BUCKET_NUMBER];

    public synchronized void set(long tid, long timestamp) {
        int position = getPosition(tid);
        Bucket bucket = buckets[position];
        if (bucket == null) {
            bucket = new Bucket();
            buckets[position] = bucket;
        }
        bucket.set(tid, timestamp);
    }

    /**
     * 
     * @param tid 事务id
     * @return -1: 事务状态未知; -2: 事务未正常提交; 其他值: 事务正常提交
     */
    public long get(long tid) {
        Bucket bucket = buckets[getPosition(tid)];
        if (bucket == null) {
            return -1;
        }
        return bucket.get(tid);
    }

    //算出tid在哪个桶
    private int getPosition(long tid) {
        return ((int) (tid / BUCKET_SIZE)) % BUCKET_NUMBER;
    }

    private static class Bucket {
        private final long tids[] = new long[BUCKET_SIZE];

        Bucket() {
            Arrays.fill(tids, -1);
        }

        long get(long tid) {
            return tids[(int) (tid % BUCKET_SIZE)];
        }

        void set(long tid, long timestamp) {
            tids[(int) (tid % BUCKET_SIZE)] = timestamp;
        }
    }
}
