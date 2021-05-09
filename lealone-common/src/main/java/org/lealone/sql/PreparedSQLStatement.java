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

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.db.session.Session;
import org.lealone.storage.PageKey;
import org.lealone.storage.replication.ReplicaSQLCommand;

public interface PreparedSQLStatement extends SQLStatement, ReplicaSQLCommand {

    public final static int MIN_PRIORITY = 1;
    public final static int NORM_PRIORITY = 5;
    public final static int MAX_PRIORITY = 10;

    Session getSession();

    String getSQL();

    int getId();

    void setId(int id);

    int getPriority();

    void setPriority(int priority);

    int getFetchSize();

    void setFetchSize(int fetchSize);

    boolean isLocal();

    void setLocal(boolean local);

    void setObjectId(int i);

    boolean canReuse();

    void reuse();

    default boolean isCacheable() {
        return false;
    }

    default boolean isDDL() {
        return false;
    }

    default boolean isIfDDL() {
        return false;
    }

    default boolean isDatabaseStatement() {
        return false;
    }

    default boolean isReplicationStatement() {
        return false;
    }

    Result query(int maxRows);

    int update();

    Yieldable<Result> createYieldableQuery(int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler);

    Yieldable<Integer> createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler);

    static interface Yieldable<T> {

        boolean run();

        void stop();

        T getResult();

        int getPriority();

        void setPageKeys(List<PageKey> pageKeys);
    }
}
