/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.util.List;

import org.lealone.storage.page.PageKey;

public class IterationParameters<K> {

    public K from;
    public K to;
    public List<PageKey> pageKeys;
    public int[] columnIndexes;
    public boolean allColumns;

    public <K2> IterationParameters<K2> copy(K2 from, K2 to) {
        IterationParameters<K2> p = new IterationParameters<>();
        p.from = from;
        p.to = to;
        p.pageKeys = pageKeys;
        p.columnIndexes = columnIndexes;
        return p;
    }

    public static <K> IterationParameters<K> create(K from) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        return p;
    }

    public static <K> IterationParameters<K> create(K from, K to) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        p.to = to;
        return p;
    }

    public static <K> IterationParameters<K> create(K from, List<PageKey> pageKeys) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        p.pageKeys = pageKeys;
        return p;
    }

    public static <K> IterationParameters<K> create(K from, int columnIndex) {
        return create(from, new int[] { columnIndex });
    }

    public static <K> IterationParameters<K> create(K from, int[] columnIndexes) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        p.columnIndexes = columnIndexes;
        return p;
    }

    public static <K> IterationParameters<K> create(K from, K to, List<PageKey> pageKeys, int[] columnIndexes) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        p.to = to;
        p.pageKeys = pageKeys;
        p.columnIndexes = columnIndexes;
        return p;
    }
}
