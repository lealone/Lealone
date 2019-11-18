/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.sql;

import java.util.List;

import org.lealone.db.Command;
import org.lealone.db.CommandParameter;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.storage.PageKey;
import org.lealone.storage.replication.ReplicationResult;

public interface SQLCommand extends Command {

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
        executeQuery(maxRows, scrollable);
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
    int executeReplicationUpdate(String replicationName, ReplicationResult replicationResult);

    default void executeUpdateAsync(AsyncHandler<AsyncResult<Integer>> handler) {
        executeUpdate();
    }
}
