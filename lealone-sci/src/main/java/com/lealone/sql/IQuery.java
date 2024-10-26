/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql;

import java.util.List;
import java.util.Set;

import com.lealone.db.command.CommandParameter;
import com.lealone.db.result.Result;

public interface IQuery {

    Result query(int maxRows);

    String getPlanSQL();

    List<? extends CommandParameter> getParameters();

    boolean allowGlobalConditions();

    void addGlobalCondition(CommandParameter param, int columnId, int indexConditionType);

    void disableCache();

    double getCost();

    Set<?> getTables();

    int getColumnCount();

    List<? extends IExpression> getExpressions();

    long getMaxDataModificationId();

    boolean isDeterministic();

}
