/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.util.List;

import org.lealone.storage.CursorParameters;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.btree.CursorPos;
import org.lealone.storage.page.PageKey;

//按page key遍历对应的page
public class PageKeyCursor<K, V> implements StorageMapCursor<K, V> {

    private final List<PageKey> pageKeys;
    private CursorPos pos;
    private K currentKey, lastKey;
    private V currentValue, lastValue;
    private int index;

    public PageKeyCursor(Page root, CursorParameters<K> parameters) {
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
    private boolean min(Page p, K from) {
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
            } else {
                x++;
            }
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
            if (min(pos.page, null)) {
                break;
            }
        }
        currentKey = null;
    }
}
