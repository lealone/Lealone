/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.util.List;

import org.lealone.storage.CursorParameters;
import org.lealone.storage.aose.btree.BTreeCursor;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.CursorPos;
import org.lealone.storage.page.PageKey;

//按page key遍历对应的page
public class PageKeyCursor<K, V> extends BTreeCursor<K, V> {

    // 可能是乱序的
    private final List<PageKey> pageKeys;
    private int index;

    public PageKeyCursor(BTreeMap<K, ?> map, CursorParameters<K> parameters) {
        this.map = map;
        this.parameters = parameters;
        this.pageKeys = parameters.pageKeys;
        // 定位到>=from的第一个PageKey对应的leaf page
        pos = min(map.getRootPage(), parameters.from);
    }

    @Override
    public boolean hasNext() {
        while (pos != null) {
            if (pos.index < pos.page.getKeyCount()) {
                return true;
            }
            pos = min(map.getRootPage(), parameters.from);
            if (pos == null) {
                return false;
            }
        }
        return false;
    }

    private CursorPos min(Page p, K from) {
        if (index >= pageKeys.size())
            return null;
        PageKey pk = pageKeys.get(index++);
        while (true) {
            if (p.isLeaf()) {
                int x = from == null ? 0 : p.binarySearch(from);
                if (x < 0) {
                    x = -x - 1;
                }
                return new CursorPos(p, x, null);
            }
            int x = p.getPageIndex(pk.key);
            if (x == 1 && pk.first && p.isLeafChildPage(x)) {
                x = 0;
            }
            p = p.getChildPage(x);
        }
    }
}
