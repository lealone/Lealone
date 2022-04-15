/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import org.lealone.storage.CursorParameters;
import org.lealone.storage.aose.btree.BTreeCursor;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.CursorPos;
import org.lealone.storage.page.PageKey;

//按page key遍历对应的page
public class PageKeyCursor<K, V> extends BTreeCursor<K, V> {

    // 会先定位到>=from的第一个PageKey对应的leaf page
    public PageKeyCursor(BTreeMap<K, ?> map, CursorParameters<K> parameters) {
        super(map, parameters);
    }

    @Override
    public boolean hasNext() {
        while (pos != null) {
            if (pos.index < pos.page.getKeyCount()) {
                return true;
            }
            // parameters.pageKeys 可能是乱序的，所以每次都从root page开始
            min(map.getRootPage(), parameters.from);
            if (pos == null) {
                return false;
            }
        }
        return false;
    }

    private int index;

    @Override
    protected void min(Page p, K from) {
        if (index >= parameters.pageKeys.size()) {
            pos = null;
            return;
        }
        PageKey pk = parameters.pageKeys.get(index++);
        while (true) {
            if (p.isLeaf()) {
                int x = from == null ? 0 : p.binarySearch(from);
                if (x < 0) {
                    x = -x - 1;
                }
                pos = new CursorPos(p, x, null);
                return;
            }
            int x = p.getPageIndex(pk.key);
            if (x == 1 && pk.first && p.isLeafChildPage(x)) {
                x = 0;
            }
            p = p.getChildPage(x);
        }
    }
}
