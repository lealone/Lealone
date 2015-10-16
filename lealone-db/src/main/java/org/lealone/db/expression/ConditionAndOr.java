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

import org.lealone.common.message.DbException;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.table.ColumnResolver;
import org.lealone.db.table.TableFilter;
import org.lealone.db.value.Value;

@SuppressWarnings("unused")
public class ConditionAndOr implements Expression {
    /**
     * The AND condition type as in ID=1 AND NAME='Hello'.
     */
    public static final int AND = 0;

    /**
     * The OR condition type as in ID=1 OR NAME='Hello'.
     */
    public static final int OR = 1;

    private final int andOrType;
    private final Expression left, right;

    public ConditionAndOr(int andOrType, Expression left, Expression right) {
        this.andOrType = andOrType;
        this.left = left;
        this.right = right;
        if (SysProperties.CHECK && (left == null || right == null)) {
            DbException.throwInternalError();
        }
    }

    @Override
    public Value getValue(Session session) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getAlias() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTableName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSchemaName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getDisplaySize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getColumnName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getPrecision() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getNullable() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isAutoIncrement() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getScale() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getSQL() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Expression getNonAliasExpression() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isConstant() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        // TODO Auto-generated method stub

    }

    @Override
    public Expression optimize(Session session) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        // TODO Auto-generated method stub

    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        // TODO Auto-generated method stub

    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        // TODO Auto-generated method stub

    }

    @Override
    public Boolean getBooleanValue(Session session) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSQL(boolean isDistributed) {
        // TODO Auto-generated method stub
        return null;
    }
}