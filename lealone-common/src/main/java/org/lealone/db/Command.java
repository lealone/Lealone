/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.util.List;

import org.lealone.db.result.Result;

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

    /**
     * Get command type.
     *
     * @return the command type.
     */
    int getType();

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
    Result query(int maxRows);

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows returned
     * @param scrollable if the result set must be scrollable
     * @return the result
     */
    Result query(int maxRows, boolean scrollable);

    /**
     * Execute the update command
     *
     * @return the update count
     */
    int update();

    /**
     * Execute the update command
     *
     * @param replicationName the replication name
     * @return the update count
     */
    int update(String replicationName);

    /**
     * Cancel the command if it is still processing.
     */
    void cancel();

    /**
     * Close the command.
     */
    void close();

    Command prepare();

}
