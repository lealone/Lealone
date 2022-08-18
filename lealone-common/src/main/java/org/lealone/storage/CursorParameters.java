/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.util.List;

import org.lealone.storage.page.PageKey;

public class CursorParameters<K> {

    public final K from;
    public final K to;
    public final List<PageKey> pageKeys;
    public final int[] columnIndexes;
    public final boolean allColumns;

    public CursorParameters(K from, K to, List<PageKey> pageKeys, int[] columnIndexes) {
        this(from, to, pageKeys, columnIndexes, false);
    }

    public CursorParameters(K from, K to, List<PageKey> pageKeys, int[] columnIndexes,
            boolean allColumns) {
        this.from = from;
        this.to = to;
        this.pageKeys = pageKeys;
        this.columnIndexes = columnIndexes;
        this.allColumns = allColumns;
    }

    public <K2> CursorParameters<K2> copy(K2 from, K2 to) {
        return new CursorParameters<>(from, to, pageKeys, columnIndexes, allColumns);
    }

    public static <K> CursorParameters<K> create(K from) {
        return new CursorParameters<>(from, null, null, null);
    }

    public static <K> CursorParameters<K> create(K from, K to) {
        return new CursorParameters<>(from, to, null, null);
    }

    public static <K> CursorParameters<K> create(K from, List<PageKey> pageKeys) {
        return new CursorParameters<>(from, null, pageKeys, null);
    }

    public static <K> CursorParameters<K> create(K from, int columnIndex) {
        return create(from, new int[] { columnIndex });
    }

    public static <K> CursorParameters<K> create(K from, int[] columnIndexes) {
        return new CursorParameters<>(from, null, null, columnIndexes);
    }

    public static <K> CursorParameters<K> create(K from, K to, List<PageKey> pageKeys,
            int[] columnIndexes) {
        return new CursorParameters<>(from, to, pageKeys, columnIndexes);
    }
}
