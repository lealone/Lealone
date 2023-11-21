/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql;

import java.util.List;

import org.lealone.db.Command;
import org.lealone.db.CommandParameter;
import org.lealone.db.async.Future;
import org.lealone.db.result.Result;

public interface SQLCommand extends Command {

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
        return executeQuery(maxRows, false);
    }

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows returned
     * @param scrollable if the result set must be scrollable
     * @return the result
     */
    Future<Result> executeQuery(int maxRows, boolean scrollable);

    /**
     * Execute the update command
     *
     * @return the update count
     */
    Future<Integer> executeUpdate();

    Future<Boolean> prepare(boolean readParams);
}
