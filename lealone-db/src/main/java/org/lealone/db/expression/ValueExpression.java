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

import org.lealone.db.Session;
import org.lealone.db.table.ColumnResolver;
import org.lealone.db.table.TableFilter;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;

public class ValueExpression implements Expression {
    /**
     * The expression represents ValueNull.INSTANCE.
     */
    private static final Object NULL = new ValueExpression(ValueNull.INSTANCE);
    /**
     * This special expression represents the default value. It is used for
     * UPDATE statements of the form SET COLUMN = DEFAULT. The value is
     * ValueNull.INSTANCE, but should never be accessed.
     */
    private static final Object DEFAULT = new ValueExpression(ValueNull.INSTANCE);
    private final Value value;

    private ValueExpression(Value value) {
        this.value = value;
    }

    /**
     * Create a new expression with the given value.
     *
     * @param value the value
     * @return the expression
     */
    public static ValueExpression get(Value value) {
        if (value == ValueNull.INSTANCE) {
            return getNull();
        }
        return new ValueExpression(value);
    }

    /**
     * Get the NULL expression.
     *
     * @return the NULL expression
     */
    public static ValueExpression getNull() {
        return (ValueExpression) NULL;
    }

    @Override
    public Value getValue(Session session) {
        return value;
    }

    @Override
    public int getType() {
        return value.getType();
    }

    @Override
    public String getSQL(boolean isDistributed) {
        if (this == DEFAULT) {
            return "DEFAULT";
        }
        return value.getSQL();
    }

    @Override
    public String getSQL() {
        return getSQL(false);
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
}
