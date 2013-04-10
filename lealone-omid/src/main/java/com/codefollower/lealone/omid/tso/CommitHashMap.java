/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.codefollower.lealone.omid.tso;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.util.internal.ConcurrentHashMap;

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

class CommitHashMap {

    private long largestDeletedTimestamp;
    private final Cache startCommitMapping;
    private final Cache rowsCommitMapping;

    /**
     * Constructs a new, empty hashtable with a default size of 1000
     */
    public CommitHashMap() {
        this(1000);
    }

    /**
     * Constructs a new, empty hashtable with the specified size
     * 
     * @param size
     *            the initial size of the hashtable.
     * @throws IllegalArgumentException
     *             if the size is less than zero.
     */
    public CommitHashMap(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Illegal size: " + size);
        }

        this.startCommitMapping = new LongCache(size, 4);
        this.rowsCommitMapping = new LongCache(size, 32);
    }

    public long getLatestWriteForRow(long hash) {
        return rowsCommitMapping.get(hash);
    }

    public void putLatestWriteForRow(long hash, long commitTimestamp) {
        long oldCommitTS = rowsCommitMapping.set(hash, commitTimestamp);
        largestDeletedTimestamp = Math.max(oldCommitTS, largestDeletedTimestamp);
    }

    public long getCommittedTimestamp(long startTimestamp) {
        return startCommitMapping.get(startTimestamp);
    }

    public void setCommittedTimestamp(long startTimestamp, long commitTimestamp) {
        long oldCommitTS = startCommitMapping.set(startTimestamp, commitTimestamp);

        largestDeletedTimestamp = Math.max(oldCommitTS, largestDeletedTimestamp);
    }

    // set of half aborted transactions
    // TODO: set the initial capacity in a smarter way
    Set<AbortedTransaction> halfAborted = Collections.newSetFromMap(new ConcurrentHashMap<AbortedTransaction, Boolean>(10000));

    private AtomicLong abortedSnapshot = new AtomicLong();

    long getAndIncrementAbortedSnapshot() {
        return abortedSnapshot.getAndIncrement();
    }

    // add a new half aborted transaction
    void setHalfAborted(long startTimestamp) {
        halfAborted.add(new AbortedTransaction(startTimestamp, abortedSnapshot.get()));
    }

    // call when a half aborted transaction is fully aborted
    void setFullAborted(long startTimestamp) {
        halfAborted.remove(new AbortedTransaction(startTimestamp, 0));
    }

    // query to see if a transaction is half aborted
    boolean isHalfAborted(long startTimestamp) {
        return halfAborted.contains(new AbortedTransaction(startTimestamp, 0));
    }

    public long getLargestDeletedTimestamp() {
        return largestDeletedTimestamp;
    }
}
