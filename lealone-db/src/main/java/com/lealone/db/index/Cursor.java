/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.index;

import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;

/**
 * A cursor is a helper object to iterate through an index.
 * For indexes are sorted (such as the b tree index), it can iterate
 * to the very end of the index. For other indexes that don't support
 * that (such as a hash index), only one row is returned.
 * The cursor is initially positioned before the first row, that means
 * next() must be called before accessing data.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface Cursor {

    /**
     * Get the complete current row.
     * All column are available.
     *
     * @return the complete row
     */
    Row get();

    default Row get(int[] columnIndexes) {
        return get();
    }

    /**
     * Get the current row.
     * Only the data for indexed columns is available in this row.
     *
     * @return the search row
     */
    default SearchRow getSearchRow() {
        return get();
    }

    /**
     * Skip to the next row if one is available.
     *
     * @return true if another row is available
     */
    boolean next();

}
