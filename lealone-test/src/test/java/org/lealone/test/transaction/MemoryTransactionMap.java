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

import java.util.Iterator;
import java.util.Map.Entry;

import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;
import org.lealone.type.DataType;

//TODO 实现所有API
public class MemoryTransactionMap<K, V> implements TransactionMap<K, V> {

    @Override
    public void setVolatile(boolean isVolatile) {

    }

    @Override
    public K lastKey() {

        return null;
    }

    @Override
    public long sizeAsLongMax() {

        return 0;
    }

    @Override
    public TransactionMap<K, V> getInstance(Transaction transaction, long savepoint) {

        return null;
    }

    @Override
    public V getLatest(K key) {

        return null;
    }

    @Override
    public V put(K key, V value) {

        return null;
    }

    @Override
    public V remove(K key) {

        return null;
    }

    @Override
    public Iterator<Entry<K, V>> entryIterator(K from) {

        return null;
    }

    @Override
    public V get(K key) {

        return null;
    }

    @Override
    public boolean isClosed() {

        return false;
    }

    @Override
    public void removeMap() {

    }

    @Override
    public void clear() {

    }

    @Override
    public K firstKey() {

        return null;
    }

    @Override
    public long sizeAsLong() {

        return 0;
    }

    @Override
    public DataType getKeyType() {

        return null;
    }

    @Override
    public V putCommitted(K key, V value) {

        return null;
    }

    @Override
    public Iterator<K> keyIterator(K from) {

        return null;
    }

    @Override
    public Iterator<K> keyIterator(K from, boolean includeUncommitted) {

        return null;
    }

    @Override
    public boolean isSameTransaction(K key) {

        return false;
    }

    @Override
    public K relativeKey(K key, long offset) {

        return null;
    }

    @Override
    public K higherKey(K key) {

        return null;
    }

    @Override
    public K lowerKey(K key) {

        return null;
    }

    @Override
    public int getMapId() {

        return 0;
    }

}
