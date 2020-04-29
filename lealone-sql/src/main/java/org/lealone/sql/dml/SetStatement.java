/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.ValueInt;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ValueExpression;

/**
 * This class represents the statement
 * SET
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class SetStatement extends ManipulationStatement {

    protected Expression expression;
    protected String stringValue;
    protected String[] stringValueList;

    public SetStatement(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.SET;
    }

    protected abstract String getSettingName();

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setStringArray(String[] list) {
        this.stringValueList = list;
    }

    public void setString(String v) {
        this.stringValue = v;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public void setInt(int value) {
        this.expression = ValueExpression.get(ValueInt.get(value));
    }

    protected int getIntValue() {
        expression = expression.optimize(session);
        return expression.getValue(session).getInt();
    }

    protected String getStringValue() {
        if (stringValue != null)
            return stringValue;
        else if (stringValueList != null)
            return StringUtils.arrayCombine(stringValueList, ',');
        else if (expression != null)
            return expression.optimize(session).getValue(session).getString();
        else
            return "";
    }

    protected int getAndValidateIntValue() {
        return getAndValidateIntValue(0);
    }

    protected int getAndValidateIntValue(int lessThan) {
        int value = getIntValue();
        if (value < lessThan) {
            throw DbException.getInvalidValueException(getSettingName(), value);
        }
        return value;
    }

    protected boolean getAndValidateBooleanValue() {
        int value = getIntValue();
        if (value < 0 || value > 1) {
            throw DbException.getInvalidValueException(getSettingName(), value);
        }
        return value == 1;
    }

    protected void databaseChanged(Database db) {
        // the meta data information has changed
        db.getNextModificationDataId();
        // query caches might be affected as well, for example
        // when changing the compatibility mode
        db.getNextModificationMetaId();
    }
}
