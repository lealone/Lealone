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
package org.lealone.sql.dml;

import org.lealone.db.ServerSession;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Expression;

public abstract class ManipulationStatement extends StatementBase {

    public ManipulationStatement(ServerSession session) {
        super(session);
    }

    @Override
    public Result getMetaData() {
        return null;
    }

    static int getLimitRows(Expression limitExpr, ServerSession session) {
        int limitRows = -1;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            if (v != ValueNull.INSTANCE) {
                limitRows = v.getInt();
            }
        }
        return limitRows;
    }
}
