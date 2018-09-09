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
package org.lealone.router;

import org.lealone.db.IDatabase;
import org.lealone.db.Session;
import org.lealone.db.result.Result;
import org.lealone.net.NetEndpoint;
import org.lealone.sql.PreparedStatement;

public class LocalRouter implements Router {

    private static final LocalRouter INSTANCE = new LocalRouter();

    public static LocalRouter getInstance() {
        return INSTANCE;
    }

    protected LocalRouter() {
    }

    @Override
    public int executeUpdate(PreparedStatement statement) {
        return statement.update();
    }

    @Override
    public Result executeQuery(PreparedStatement statement, int maxRows) {
        return statement.query(maxRows);
    }

    @Override
    public String[] getHostIds(IDatabase db) {
        return new String[] { NetEndpoint.getLocalTcpHostAndPort() };
    }

    @Override
    public int executeDatabaseStatement(IDatabase db, Session currentSession, PreparedStatement statement) {
        return 0;
    }
}
