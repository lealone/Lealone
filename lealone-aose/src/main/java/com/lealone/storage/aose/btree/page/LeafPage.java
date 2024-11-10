/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import com.lealone.storage.aose.btree.BTreeMap;

public abstract class LeafPage extends LocalPage {

    protected LeafPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return keys == null || keys.length == 0;
    }
}
