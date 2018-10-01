/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.util.List;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.storage.PageKey;

/**
 * Represents a command.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface Command {

    int CLIENT_COMMAND = -1;
    int CLIENT_BATCH_COMMAND = -2;
    int SERVER_COMMAND = -3;
    int REPLICATION_COMMAND = -4;

    /**
     * Get command type.
     *
     * @return the command type.
     */
    int getType();

    /**
     * Cancel the command if it is still processing.
     */
    void cancel();

    /**
     * Close the command.
     */
    void close();

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
    Result getMetaData();

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
    Result executeQuery(int maxRows);

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows returned
     * @param scrollable if the result set must be scrollable
     * @return the result
     */
    Result executeQuery(int maxRows, boolean scrollable);

    default Result executeQuery(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        return executeQuery(maxRows, scrollable);
    }

    default void executeQueryAsync(int maxRows, boolean scrollable, AsyncHandler<AsyncResult<Result>> handler) {
        executeQueryAsync(maxRows, scrollable, null, handler);
    }

    default void executeQueryAsync(int maxRows, boolean scrollable, List<PageKey> pageKeys,
            AsyncHandler<AsyncResult<Result>> handler) {
        Result result = executeQuery(maxRows, scrollable, pageKeys);
        if (handler != null) {
            AsyncResult<Result> r = new AsyncResult<>();
            r.setResult(result);
            handler.handle(r);
        }
    }

    /**
     * Execute the update command
     *
     * @return the update count
     */
    int executeUpdate();

    default int executeUpdate(List<PageKey> pageKeys) {
        return executeUpdate();
    }

    /**
     * Execute the update command
     *
     * @param replicationName the replication name
     * @return the update count
     */
    int executeUpdate(String replicationName, CommandUpdateResult commandUpdateResult);

    default void executeUpdateAsync(AsyncHandler<AsyncResult<Integer>> handler) {
        executeUpdateAsync(null, handler);
    }

    default void executeUpdateAsync(List<PageKey> pageKeys, AsyncHandler<AsyncResult<Integer>> handler) {
        int updateCount = executeUpdate(pageKeys);
        if (handler != null) {
            AsyncResult<Integer> r = new AsyncResult<>();
            r.setResult(updateCount);
            handler.handle(r);
        }
    }

    default Command prepare() {
        return this;
    }

    default void replicationCommit(long validKey, boolean autoCommit) {
    }

    default void replicationRollback() {
    }
}
