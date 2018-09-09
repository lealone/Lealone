/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.aose.btree;

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
    public final BTreePage page;

    /**
     * The current index.
     */
    public int index;

    /**
     * The position in the parent page, if any.
     */
    public final CursorPos parent;

    public CursorPos(BTreePage page, int index, CursorPos parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }

}
