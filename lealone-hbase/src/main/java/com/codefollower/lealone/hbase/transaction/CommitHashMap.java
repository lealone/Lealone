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

/**
 * This class stores the mapping between start a commit timestamps and between
 * modified row and commit timestamp.
 * 
 * Both mappings are respresented as a long->long mapping, each of them
 * implemented using a single long []
 * 
 * For a map of size N we create an array of size 2*N and store the keys on even
 * indexes and values on odd indexes.
 * 
 * Each time an entry is removed, we update the largestDeletedTimestamp if the
 * entry's commit timestamp is greater than this value.
 * 
 * Rationale: we want queries to be fast and touch as least memory regions as
 * possible
 * 
 * TODO: improve garbage collection, right now an entry is picked at random (by
 * hash) which could cause the eviction of a very recent timestamp
 */

public class CommitHashMap {
    private final LongCache rowsCommitMapping;
    private long largestDeletedTimestamp;

    /**
     * Constructs a new, empty hashtable with the specified size and associativity
     * 
     * @param size
     *            the initial size of the hashtable.
     * @param associativity
     *            the associativity of the cache.
     * @throws IllegalArgumentException
     *             if the size or associativity is less than zero.
     */
    public CommitHashMap(int size, int associativity) {
        if (size < 0) {
            throw new IllegalArgumentException("Illegal size: " + size);
        }
        if (associativity < 0) {
            throw new IllegalArgumentException("Illegal associativity: " + associativity);
        }
        this.rowsCommitMapping = new LongCache(size, associativity);
    }

    public long getLatestWriteForRow(long hash) {
        return rowsCommitMapping.get(hash);
    }

    public void putLatestWriteForRow(long hash, long commitTimestamp) {
        long oldCommitTS = rowsCommitMapping.set(hash, commitTimestamp);
        largestDeletedTimestamp = Math.max(oldCommitTS, largestDeletedTimestamp);
    }

    public long getLargestDeletedTimestamp() {
        return largestDeletedTimestamp;
    }

    public static class LongCache {
        private final long[] cache;
        private final int associativity;
        private final int mask;

        public LongCache(int size, int associativity) {
            this.cache = new long[2 * (size + associativity)];
            this.associativity = associativity;
            this.mask = size - 1;
        }

        public long set(long key, long value) {
            final int index = index(key);
            int oldestIndex = 0;
            long oldestValue = Long.MAX_VALUE;
            for (int i = 0; i < associativity; ++i) {
                int currIndex = 2 * (index + i);
                if (cache[currIndex] == key) {
                    oldestValue = 0;
                    oldestIndex = currIndex;
                    break;
                }
                if (cache[currIndex + 1] <= oldestValue) {
                    oldestValue = cache[currIndex + 1];
                    oldestIndex = currIndex;
                }
            }
            cache[oldestIndex] = key;
            cache[oldestIndex + 1] = value;
            return oldestValue;
        }

        public long get(long key) {
            final int index = index(key);
            for (int i = 0; i < associativity; ++i) {
                int currIndex = 2 * (index + i);
                if (cache[currIndex] == key) {
                    return cache[currIndex + 1];
                }
            }
            return 0;
        }

        private int index(long hash) {
            return (int) (Math.abs(hash) & mask);
        }
    }
}
