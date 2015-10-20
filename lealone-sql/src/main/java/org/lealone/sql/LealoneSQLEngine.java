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

import java.util.Map;

import org.lealone.db.CommandParameter;
import org.lealone.db.Constants;
import org.lealone.db.Session;
import org.lealone.db.schema.Sequence;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.SequenceValue;
import org.lealone.sql.expression.ValueExpression;

public class LealoneSQLEngine implements SQLEngine {

    public LealoneSQLEngine() {
        SQLEngineManager.getInstance().registerEngine(this);
    }

    @Override
    public SQLParser createParser(Session session) {
        return new Parser((org.lealone.db.ServerSession) session);
    }

    @Override
    public String getName() {
        return Constants.DEFAULT_SQL_ENGINE_NAME;
    }

    @Override
    public void init(Map<String, String> config) {
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return Parser.quoteIdentifier(identifier);
    }

    @Override
    public CommandParameter createParameter(int index) {
        return new Parameter(index);
    }

    @Override
    public Expression createValueExpression(Value value) {
        return ValueExpression.get(value);
    }

    @Override
    public Expression createSequenceValue(Object sequence) {
        return new SequenceValue((Sequence) sequence);
    }
}
