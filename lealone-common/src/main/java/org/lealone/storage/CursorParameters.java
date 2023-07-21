/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

public class CursorParameters<K> {

    public final K from;
    public final K to;
    public final int[] columnIndexes;
    public final boolean allColumns;
    public boolean forUpdate;

    public CursorParameters(K from, K to, int[] columnIndexes) {
        this(from, to, columnIndexes, false);
    }

    public CursorParameters(K from, K to, int[] columnIndexes, boolean allColumns) {
        this.from = from;
        this.to = to;
        this.columnIndexes = columnIndexes;
        this.allColumns = allColumns;
    }

    public <K2> CursorParameters<K2> copy(K2 from, K2 to) {
        return new CursorParameters<>(from, to, columnIndexes, allColumns);
    }

    public static <K> CursorParameters<K> create(K from) {
        return new CursorParameters<>(from, null, null);
    }

    public static <K> CursorParameters<K> create(K from, K to) {
        return new CursorParameters<>(from, to, null);
    }

    public static <K> CursorParameters<K> create(K from, int columnIndex) {
        return create(from, new int[] { columnIndex });
    }

    public static <K> CursorParameters<K> create(K from, int[] columnIndexes) {
        return new CursorParameters<>(from, null, columnIndexes);
    }

    public static <K> CursorParameters<K> create(K from, K to, int[] columnIndexes) {
        return new CursorParameters<>(from, to, columnIndexes);
    }
}
