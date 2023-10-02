/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.schema.SchemaObject;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.storage.CursorParameters;

/**
 * An index. Indexes are used to speed up searching data.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface Index extends SchemaObject {

    /**
     * Get the table on which this index is based.
     *
     * @return the table
     */
    Table getTable();

    /**
     * Get the index type.
     *
     * @return the index type
     */
    IndexType getIndexType();

    /**
     * Get the indexed columns as index columns (with ordering information).
     *
     * @return the index columns
     */
    IndexColumn[] getIndexColumns();

    /**
     * Get the indexed columns.
     *
     * @return the columns
     */
    Column[] getColumns();

    /**
     * Get the indexed column ids.
     *
     * @return the column ids
     */
    int[] getColumnIds();

    /**
     * Get the index of a column in the list of index columns
     *
     * @param col the column
     * @return the index (0 meaning first column)
     */
    int getColumnIndex(Column col);

    /**
     * Get the message to show in a EXPLAIN statement.
     *
     * @return the plan
     */
    String getPlanSQL();

    /**
     * Add a row to the index.
     *
     * @param session the session to use
     * @param row the row to add
     */
    default Future<Integer> add(ServerSession session, Row row) {
        throw DbException.getUnsupportedException("add row");
    }

    default Future<Integer> update(ServerSession session, Row oldRow, Row newRow, int[] updateColumns,
            boolean isLockedBySelf) {
        AsyncCallback<Integer> ac = session.createCallback();
        remove(session, oldRow, isLockedBySelf).onSuccess(v -> {
            add(session, newRow).onComplete(ar -> {
                ac.setAsyncResult(ar);
            });
        }).onFailure(t -> {
            ac.setAsyncResult(t);
        });
        return ac;
    }

    /**
     * Remove a row from the index.
     *
     * @param session the session
     * @param row the row
     */
    default Future<Integer> remove(ServerSession session, Row row) {
        return remove(session, row, false);
    }

    default Future<Integer> remove(ServerSession session, Row row, boolean isLockedBySelf) {
        throw DbException.getUnsupportedException("remove row");
    }

    default int tryLock(ServerSession session, Row row, int[] lockColumns) {
        return 0;
    }

    /**
     * Find a row or a list of rows and create a cursor to iterate over the result.
     *
     * @param session the session
     * @param first the first row, or null for no limit
     * @param last the last row, or null for no limit
     * @return the cursor to iterate over the results
     */
    Cursor find(ServerSession session, SearchRow first, SearchRow last);

    Cursor find(ServerSession session, CursorParameters<SearchRow> parameters);

    /**
     * Check if the index can directly look up the lowest or highest value of a
     * column.
     *
     * @return true if it can
     */
    boolean canGetFirstOrLast();

    /**
     * Find the first (or last) value of this index.
     *
     * @param session the session
     * @param first true if the first (lowest for ascending indexes) or last
     *            value should be returned
     * @return a SearchRow or null
     */
    SearchRow findFirstOrLast(ServerSession session, boolean first);

    /**
     * Check if the index supports distinct query.
     *
     * @return true if it supports
     */
    boolean supportsDistinctQuery();

    /**
     * Find a distinct list of rows and create a cursor to iterate over the result.
     *
     * @param session the session
     * @return the cursor to iterate over the results
     */
    Cursor findDistinct(ServerSession session);

    /**
     * Can this index iterate over all rows?
     *
     * @return true if it can
     */
    boolean canScan();

    /**
     * Does this index support lookup by row id?
     *
     * @return true if it does
     */
    boolean isRowIdIndex();

    /**
     * Get the row with the given key.
     *
     * @param session the session
     * @param key the unique key
     * @return the row
     */
    Row getRow(ServerSession session, long key);

    /**
     * Compare two rows.
     *
     * @param rowData the first row
     * @param compare the second row
     * @return 0 if both rows are equal, -1 if the first row is smaller, otherwise 1
     */
    int compareRows(SearchRow rowData, SearchRow compare);

    /**
     * Estimate the cost to search for rows given the search mask.
     * There is one element per column in the search mask.
     * For possible search masks, see IndexCondition.
     *
     * @param session the session
     * @param masks per-column comparison bit masks, null means 'always false',
     *              see constants in IndexCondition
     * @param filter the table filter
     * @param sortOrder the sort order
     * @return the estimated cost
     */
    double getCost(ServerSession session, int[] masks, SortOrder sortOrder);

    /**
     * Get the row count of this table, for the given session.
     *
     * @param session the session
     * @return the row count
     */
    long getRowCount(ServerSession session);

    /**
     * Get the approximated row count for this table.
     *
     * @return the approximated row count
     */
    long getRowCountApproximation();

    /**
     * Close this index.
     *
     * @param session the session used to write data
     */
    void close(ServerSession session);

    /**
     * Remove the index.
     *
     * @param session the session
     */
    void remove(ServerSession session);

    /**
     * Remove all rows from the index.
     *
     * @param session the session
     */
    void truncate(ServerSession session);

    /**
     * Get the used disk space for this index.
     *
     * @return the estimated number of bytes
     */
    long getDiskSpaceUsed();

    /**
     * Get the used memory space for this index.
     *
     * @return the estimated number of bytes
     */
    long getMemorySpaceUsed();

    /**
     * Check if the index needs to be rebuilt.
     * This method is called after opening an index.
     *
     * @return true if a rebuild is required.
     */
    boolean needRebuild();

    boolean isInMemory();
}
