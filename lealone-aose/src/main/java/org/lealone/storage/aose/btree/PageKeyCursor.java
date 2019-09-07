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
package org.lealone.storage.aose.btree;

import java.util.List;

import org.lealone.storage.IterationParameters;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageMapCursor;

//按page key遍历对应的page
class PageKeyCursor<K, V> implements StorageMapCursor<K, V> {

    private final List<PageKey> pageKeys;
    private CursorPos pos;
    private K currentKey, lastKey;
    private V currentValue, lastValue;
    private int index;

    PageKeyCursor(List<PageKey> pageKeys, BTreePage root, K from) {
        this.pageKeys = pageKeys;
        // 提前fetch
        min(root, from);
        fetchNext();
    }

    PageKeyCursor(BTreePage root, IterationParameters<K> parameters) {
        this.pageKeys = parameters.pageKeys;
        // 提前fetch
        min(root, parameters.from);
        fetchNext();
    }

    @Override
    public K getKey() {
        return lastKey;
    }

    @Override
    public V getValue() {
        return lastValue;
    }

    @Override
    public boolean hasNext() {
        return currentKey != null;
    }

    @Override
    public K next() {
        K c = currentKey;
        lastKey = currentKey;
        lastValue = currentValue;
        fetchNext();
        return c;
    }

    /**
    * Fetch the next entry that is equal or larger than the given key, starting
    * from the given page. This method retains the stack.
    *
    * @param p the page to start
    * @param from the key to search
    */
    private boolean min(BTreePage p, K from) {
        if (index >= pageKeys.size())
            return true;
        PageKey pk = pageKeys.get(index++);
        while (true) {
            if (p.isLeaf()) {
                int x = from == null ? 0 : p.binarySearch(from);
                if (x < 0) {
                    x = -x - 1;
                }
                pos = new CursorPos(p, x, pos);
                break;
            }
            // int x = from == null ? -1 : p.binarySearch(from);
            int x = p.binarySearch(pk.key);
            if (x < 0) {
                x = -x - 1;
                // if (x == p.getKeyCount()) {
                // if (pos != null) {
                // pos = pos.parent;
                // if (pos == null) {
                // break;
                // }
                // p = pos.page;
                // continue;
                // }
                // }
            } else {
                x++;
            }
            // while (x < map.getChildPageCount(p) && p.isRemoteChildPage(x)) {
            // x++;
            // }
            // if (x == map.getChildPageCount(p)) {
            // if (p.isRemoteChildPage(x - 1)) {
            // pos = null;
            // currentKey = null;
            // break;
            // } else
            // x--;
            // }
            if (pk.first && p.isLeafChildPage(x)) {
                x = 0;
            }
            if (pos == null || pos.page != p)
                pos = new CursorPos(p, x + 1, pos);
            p = p.getChildPage(x);
        }
        return false;
    }

    /**
    * Fetch the next entry if there is one.
    */
    @SuppressWarnings("unchecked")
    private void fetchNext() {
        while (pos != null) {
            if (pos.index < pos.page.getKeyCount()) {
                int index = pos.index++;
                currentKey = (K) pos.page.getKey(index);
                currentValue = (V) pos.page.getValue(index);
                return;
            }
            pos = pos.parent;
            if (pos == null) {
                break;
            }
            // if (pos.index < map.getChildPageCount(pos.page)) {
            // // while (pos.index < map.getChildPageCount(pos.page) && pos.page.isRemoteChildPage(pos.index)) {
            // // pos.index++;
            // // }
            // // if (pos.index == map.getChildPageCount(pos.page)) {
            // // if (pos.page.isRemoteChildPage(pos.index - 1)) {
            // // pos = null;
            // // currentKey = null;
            // // break;
            // // } else
            // // pos.index--;
            // // }
            // // min(pos.page.getChildPage(pos.index++), null);
            // }
            if (min(pos.page, null)) {
                break;
            }
        }
        currentKey = null;
    }

    // /**
    // * Fetch the next entry that is equal or larger than the given key, starting
    // * from the given page. This method retains the stack.
    // *
    // * @param p the page to start
    // * @param from the key to search
    // */
    // private boolean min(BTreePage p, K from) {
    // if (index >= pageKeys.size())
    // return true;
    // p = root;
    // PageKey pk = pageKeys.get(index++);
    // while (true) {
    // if (p.isLeaf()) {
    // int x = from == null ? 0 : p.binarySearch(from);
    // if (x < 0) {
    // x = -x - 1;
    // }
    // pos = new CursorPos(p, x, pos);
    // break;
    // }
    // // int x = from == null ? -1 : p.binarySearch(from);
    // int x = p.binarySearch(pk.key);
    // if (x < 0) {
    // x = -x - 1;
    // // if (x == p.getKeyCount()) {
    // // pos = pos.parent;
    // // if (pos == null) {
    // // break;
    // // }
    // // p = pos.page;
    // // continue;
    // // }
    // } else {
    // x++;
    // }
    // // while (x < map.getChildPageCount(p) && p.isRemoteChildPage(x)) {
    // // x++;
    // // }
    // // if (x == map.getChildPageCount(p)) {
    // // if (p.isRemoteChildPage(x - 1)) {
    // // pos = null;
    // // currentKey = null;
    // // break;
    // // } else
    // // x--;
    // // }
    // if (pk.first && p.isLeafChildPage(x)) {
    // x = 0;
    // }
    // // if (pos == null || pos.page != p)
    // // pos = new CursorPos(p, x + 1, pos);
    // p = p.getChildPage(x);
    // }
    // return false;
    // }
    //
    // /**
    // * Fetch the next entry if there is one.
    // */
    // @SuppressWarnings("unchecked")
    // private void fetchNext() {
    // while (pos != null) {
    // if (pos.index < pos.page.getKeyCount()) {
    // int index = pos.index++;
    // currentKey = (K) pos.page.getKey(index);
    // currentValue = (V) pos.page.getValue(index);
    // return;
    // }
    // // pos = pos.parent;
    // // if (pos == null) {
    // // break;
    // // }
    // // // if (pos.index < map.getChildPageCount(pos.page)) {
    // // // // while (pos.index < map.getChildPageCount(pos.page) && pos.page.isRemoteChildPage(pos.index)) {
    // // // // pos.index++;
    // // // // }
    // // // // if (pos.index == map.getChildPageCount(pos.page)) {
    // // // // if (pos.page.isRemoteChildPage(pos.index - 1)) {
    // // // // pos = null;
    // // // // currentKey = null;
    // // // // break;
    // // // // } else
    // // // // pos.index--;
    // // // // }
    // // // // min(pos.page.getChildPage(pos.index++), null);
    // // // }
    // // if (min(pos.page, null)) {
    // // break;
    // // }
    //
    // if (min(null, null)) {
    // break;
    // }
    // }
    // currentKey = null;
    // }
}
