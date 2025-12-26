/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.command;

import java.util.List;

import com.lealone.db.async.Future;
import com.lealone.db.result.Result;
import com.lealone.db.value.Value;

public interface SQLCommand extends Command {

    String getSQL();

    int getFetchSize();

    void setFetchSize(int fetchSize);

    /**
     * Get the parameters (if any).
     *
     * @return the parameters
     */
    List<? extends CommandParameter> getParameters();

    /**
     * Get an empty result set containing the meta data of the result.
     *
     * @return the empty result
     */
    Future<Result> getMetaData();

    /**
     * Check if this is a query.
     *
     * @return true if it is a query
     */
    boolean isQuery();

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows returned
     * @return the result
     */
    default Future<Result> executeQuery(int maxRows) {
        return executeQuery(maxRows, false, null);
    }

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows returned
     * @param scrollable if the result set must be scrollable
     * @return the result
     */
    default Future<Result> executeQuery(int maxRows, boolean scrollable) {
        return executeQuery(maxRows, scrollable, null);
    }

    Future<Result> executeQuery(int maxRows, boolean scrollable, Value[] parameterValues);

    /**
     * Execute the update command
     *
     * @return the update count
     */
    Future<Integer> executeUpdate();

    Future<Integer> executeUpdate(Value[] parameterValues);

    Future<Boolean> prepare(boolean readParams);
}
