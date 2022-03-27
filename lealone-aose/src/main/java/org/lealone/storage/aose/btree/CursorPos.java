/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import org.lealone.storage.aose.btree.page.Page;

/**
 * A position in a cursor
 * 
 * @author H2 Group
 * @author zhh
 */
public class CursorPos {

    /**
     * The current page.
     */
    public final Page page;

    /**
     * The current index.
     */
    public int index;

    /**
     * The position in the parent page, if any.
     */
    public final CursorPos parent;

    public CursorPos(Page page, int index, CursorPos parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }
}
