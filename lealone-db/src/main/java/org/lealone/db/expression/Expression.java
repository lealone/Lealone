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
package org.lealone.db.expression;

import org.lealone.db.ServerSession;
import org.lealone.db.table.ColumnResolver;
import org.lealone.db.table.TableFilter;
import org.lealone.db.value.Value;

public interface Expression extends org.lealone.sql.Expression {
    Value getValue(ServerSession session);

    String getAlias();

    String getTableName();

    String getSchemaName();

    int getDisplaySize();

    String getColumnName();

    @Override
    int getType();

    long getPrecision();

    int getNullable();

    boolean isAutoIncrement();

    int getScale();

    @Override
    String getSQL();

    String getSQL(boolean isDistributed);

    Expression getNonAliasExpression();

    boolean isConstant();

    boolean isEverything(ExpressionVisitor visitor);

    void setEvaluatable(TableFilter tableFilter, boolean value);

    Expression optimize(ServerSession session);

    void addFilterConditions(TableFilter filter, boolean outerJoin);

    void mapColumns(ColumnResolver resolver, int level);

    void createIndexConditions(ServerSession session, TableFilter filter);

    Boolean getBooleanValue(ServerSession session);
}
