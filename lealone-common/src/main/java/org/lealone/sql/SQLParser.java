/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql;

public interface SQLParser {

    void setRightsChecked(boolean rightsChecked);

    IExpression parseExpression(String sql);

    ParsedSQLStatement parse(String sql);

    Object parseColumnForTable(String columnSql);

}
