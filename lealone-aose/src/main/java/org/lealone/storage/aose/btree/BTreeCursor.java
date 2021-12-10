/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.aose.btree;

import org.lealone.storage.IterationParameters;
import org.lealone.storage.StorageMapCursor;

/**
 * A cursor to iterate over elements in ascending order.
 * 
 * @param <K> the key type
 * @param <V> the value type
 * 
 * @author H2 Group
 * @author zhh
 */
class BTreeCursor<K, V> implements StorageMapCursor<K, V> {

    private final BTreeMap<K, ?> map;
    private final IterationParameters<K> parameters;

    private CursorPos pos;
    private K key;
    private V value;

    BTreeCursor(BTreeMap<K, ?> map, BTreePage root, IterationParameters<K> parameters) {
        this.map = map;
        this.parameters = parameters;
        // 定位到>=from的第一个leaf page
        min(root, parameters.from);
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public boolean hasNext() {
        while (pos != null) {
            if (pos.index < pos.page.getKeyCount()) {
                return true;
            }
            pos = pos.parent;
            if (pos == null) {
                return false;
            }
            if (pos.index < map.getChildPageCount(pos.page)) {
                min(pos.page.getChildPage(pos.index++), null);
            }
        }
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K next() {
        // 不再检测了，让调用者自己先调用hasNext()再调用next()
        // if (!hasNext()) {
        // throw new NoSuchElementException();
        // }
        int index = pos.index++;
        key = (K) pos.page.getKey(index);
        if (parameters.allColumns)
            value = (V) pos.page.getValue(index, true);
        else
            value = (V) pos.page.getValue(index, parameters.columnIndexes);
        return key;
    }

    /**
     * Fetch the next entry that is equal or larger than the given key, starting
     * from the given page. This method retains the stack.
     * 
     * @param p the page to start
     * @param from the key to search
     */
    private void min(BTreePage p, K from) {
        while (true) {
            if (p.isLeaf()) {
                p = p.tmpCopyIfSplited();
                int x = from == null ? 0 : p.binarySearch(from);
                if (x < 0) {
                    x = -x - 1;
                }
                pos = new CursorPos(p, x, pos);
                break;
            }
            int x = from == null ? -1 : p.binarySearch(from);
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            pos = new CursorPos(p, x + 1, pos);
            p = p.getChildPage(x);
        }
    }
}
