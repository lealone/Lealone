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

package com.codefollower.yourbase.omid.tso;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.util.internal.ConcurrentHashMap;

/**
 * A hash map that uses byte[] for the key rather than longs.
 * 
 * Change it to lazyly clean the old entries, i.e., upon a hit This would reduce
 * the mem access benefiting from cache locality
 * 
 * @author maysam
 */

class CommitHashMap {

    native void init(int initialCapacity, int maxCommits, float loadFactor);

    native static long gettotalput();

    native static long gettotalget();

    native static long gettotalwalkforput();

    native static long gettotalwalkforget();

    // Load the library
    static {
        System.loadLibrary("tso-commithashmap");
    }

    /**
     * Constructs a new, empty hashtable with a default capacity and load factor,
     * which is <code>1000</code> and <code>0.75</code> respectively.
     */
    public CommitHashMap() {
        this(1000, 0.75f);
    }

    /**
     * Constructs a new, empty hashtable with the specified initial capacity and
     * default load factor, which is <code>0.75</code>.
     * 
     * @param initialCapacity
     *           the initial capacity of the hashtable.
     * @throws IllegalArgumentException
     *            if the initial capacity is less than zero.
     */
    public CommitHashMap(int initialCapacity) {
        this(initialCapacity, 0.75f);
    }

    /**
     * Constructs a new, empty hashtable with the specified initial capacity and
     * the specified load factor.
     * 
     * @param initialCapacity
     *           the initial capacity of the hashtable.
     * @param loadFactor
     *           the load factor of the hashtable.
     * @throws IllegalArgumentException
     *            if the initial capacity is less than zero, or if the load
     *            factor is nonpositive.
     */
    public CommitHashMap(int initialCapacity, float loadFactor) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
        }
        if (loadFactor <= 0) {
            throw new IllegalArgumentException("Illegal Load: " + loadFactor);
        }
        if (initialCapacity == 0) {
            initialCapacity = 1;
        }

        //assuming the worst case that each transaction modifies a value, this is the right size because it is proportional to the hashmap size
        int txnCommitArraySize = (int) (initialCapacity * loadFactor);
        this.init(initialCapacity, txnCommitArraySize, loadFactor);
    }

    /**
     * Returns the value to which the specified key is mapped in this map. If
     * there are multiple values with the same key, return the first The first is
     * the one with the largest key, because (i) put always put the recent ones
     * ahead, (ii) a new put on the same key has always larger value (because
     * value is commit timestamp and the map is atmoic)
     * 
     * @param key
     *           a key in the hashtable.
     * @return the value to which the key is mapped in this hashtable;
     *         <code>null</code> if the key is not mapped to any value in this
     *         hashtable.
     * @see #put(int, Object)
     */
    native long get(byte[] rowId, byte[] tableId, int hash);

    /**
     * Maps the specified <code>key</code> to the specified <code>value</code> in
     * this hashtable. The key cannot be <code>null</code>.
     * 
     * The value can be retrieved by calling the <code>get</code> method with a
     * key that is equal to the original key.
     * 
     * It guarantees that if multiple entries with the same keys exist then the
     * first one is the most fresh one, i.e., with the largest value
     * 
     * @param key
     *           the hashtable key.
     * @param value
     *           the value.
     * @throws NullPointerException
     *            if the key is <code>null</code>. return true if the vlaue is
     *            replaced
     */
    native long put(byte[] rowId, byte[] tableId, long value, int hash, long largestDeletedTimestamp);

    /**
     * Returns the commit timestamp 
     *
     * @param   startTimestamp   the transaction start timestamp
     * @return  commit timestamp if such mapping exist, 0 otherwise
     */
    native long getCommittedTimestamp(long startTimestamp);

    native long setCommitted(long startTimestamp, long commitTimestamp, long largestDeletedTimestamp);

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
}
