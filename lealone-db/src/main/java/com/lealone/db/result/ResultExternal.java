/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.result;

import java.util.ArrayList;

import com.lealone.db.value.Value;

/**
 * This interface is used to extend the LocalResult class, if data does not fit in memory.
 */
public interface ResultExternal {

    /**
     * Reset the current position of this object.
     */
    void reset();

    /**
     * Get the next row from the result.
     *
     * @return the next row or null
     */
    Value[] next();

    /**
     * Add a row to this object.
     *
     * @param values the row to add
     * @return the new number of rows in this object
     */
    int addRow(Value[] values);

    /**
     * Add a number of rows to the result.
     *
     * @param rows the list of rows to add
     * @return the new number of rows in this object
     */
    int addRows(ArrayList<Value[]> rows);

    /**
     * This method is called after all rows have been added.
     */
    void done();

    /**
     * Close this object and delete the temporary file.
     */
    void close();

    /**
     * Remove the row with the given values from this object if such a row exists.
     *
     * @param values the row
     * @return the new row count
     */
    int removeDistinct(Value[] values);

    /**
     * Check if the given row exists in this object.
     *
     * @param values the row
     * @return true if it exists
     */
    boolean containsDistinct(Value[] values);

    /**
     * Create a shallow copy of this object if possible.
     *
     * @return the shallow copy, or null
     */
    ResultExternal createShallowCopy();

}
