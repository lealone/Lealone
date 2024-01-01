/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql;

public interface ParsedSQLStatement extends SQLStatement {

    PreparedSQLStatement prepare();

}
