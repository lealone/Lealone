/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.lealone.aostore.btree;

import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.common.util.DataUtils;

public class BufferedBTreePage extends BTreePage {
    static class ValueHolder {
        final Object value;

        ValueHolder(Object value) {
            this.value = value;
        }
    }

    // volatile ConcurrentSkipListMap<Object, ValueHolder> current = new ConcurrentSkipListMap<>();

    volatile ConcurrentSkipListMap<Object, Object> current = new ConcurrentSkipListMap<>();

    ConcurrentSkipListMap<Object, Object>[] buffers;

    BufferedBTreePage(BTreeMap<?, ?> map, long version) {
        super(map, version);
    }

    /**
     * Create a new, empty page.
     * 
     * @param map the map
     * @param version the version
     * @return the new page
     */
    static BufferedBTreePage createEmpty(BTreeMap<?, ?> map, long version) {
        return create(map, version, EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY, null, 0, DataUtils.PAGE_MEMORY);
    }

    /**
     * Create a new page. The arrays are not cloned.
     * 
     * @param map the map
     * @param version the version
     * @param keys the keys
     * @param values the values
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static BufferedBTreePage create(BTreeMap<?, ?> map, long version, Object[] keys, Object[] values,
            PageReference[] children, long totalCount, int memory) {
        BufferedBTreePage p = new BufferedBTreePage(map, version);
        // the position is 0
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.totalCount = totalCount;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        BTreeStore store = map.store;
        if (store != null) {
            store.registerUnsavedPage(p.memory);
        }
        return p;
    }

    Object uncommitKey;

    public void commit() {

    }

    void setUncommitKey() {

    }
}
