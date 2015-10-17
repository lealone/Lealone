/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.util.ArrayList;

import org.lealone.db.result.Result;

/**
 * Represents a SQL statement.
 */
public interface Command {

    int COMMAND = -1;

    /**
     * Get command type.
     *
     * @return one of the constants above
     */
    int getType();

    /**
    * Check if this is a query.
    *
    * @return true if it is a query
    */
    boolean isQuery();

    /**
    * Get the parameters (if any).
    *
    * @return the parameters
    */
    ArrayList<? extends CommandParameter> getParameters();

    /**
    * Execute the query.
    *
    * @param maxRows the maximum number of rows returned
    * @param scrollable if the result set must be scrollable
    * @return the result
    */
    Result executeQuery(int maxRows, boolean scrollable);

    /**
    * Execute the statement
    *
    * @return the update count
    */
    int executeUpdate();

    /**
    * Close the statement.
    */
    void close();

    /**
    * Cancel the statement if it is still processing.
    */
    void cancel();

    /**
    * Get an empty result set containing the meta data of the result.
    *
    * @return the empty result
    */
    Result getMetaData();
}
