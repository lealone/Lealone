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

import org.lealone.db.Constants;
import org.lealone.db.SessionInterface;

public class LealoneSQLEngine implements SQLEngine {
    public LealoneSQLEngine() {
        SQLEngineManager.registerSQLEngine(this);
    }

    @Override
    public SQLParser createParser(SessionInterface session) {
        return new Parser((org.lealone.db.Session) session);
    }

    @Override
    public String getName() {
        return Constants.DEFAULT_SQL_ENGINE_NAME;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return Parser.quoteIdentifier(identifier);
    }

}
