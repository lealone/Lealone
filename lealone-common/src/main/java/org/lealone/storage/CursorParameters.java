/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.util.List;

import org.lealone.storage.page.PageKey;

public class CursorParameters<K> {

    public K from;
    public K to;
    public List<PageKey> pageKeys;
    public int[] columnIndexes;
    public boolean allColumns;

    public <K2> CursorParameters<K2> copy(K2 from, K2 to) {
        CursorParameters<K2> p = new CursorParameters<>();
        p.from = from;
        p.to = to;
        p.pageKeys = pageKeys;
        p.columnIndexes = columnIndexes;
        return p;
    }

    public static <K> CursorParameters<K> create(K from) {
        CursorParameters<K> p = new CursorParameters<>();
        p.from = from;
        return p;
    }

    public static <K> CursorParameters<K> create(K from, K to) {
        CursorParameters<K> p = new CursorParameters<>();
        p.from = from;
        p.to = to;
        return p;
    }

    public static <K> CursorParameters<K> create(K from, List<PageKey> pageKeys) {
        CursorParameters<K> p = new CursorParameters<>();
        p.from = from;
        p.pageKeys = pageKeys;
        return p;
    }

    public static <K> CursorParameters<K> create(K from, int columnIndex) {
        return create(from, new int[] { columnIndex });
    }

    public static <K> CursorParameters<K> create(K from, int[] columnIndexes) {
        CursorParameters<K> p = new CursorParameters<>();
        p.from = from;
        p.columnIndexes = columnIndexes;
        return p;
    }

    public static <K> CursorParameters<K> create(K from, K to, List<PageKey> pageKeys, int[] columnIndexes) {
        CursorParameters<K> p = new CursorParameters<>();
        p.from = from;
        p.to = to;
        p.pageKeys = pageKeys;
        p.columnIndexes = columnIndexes;
        return p;
    }
}
