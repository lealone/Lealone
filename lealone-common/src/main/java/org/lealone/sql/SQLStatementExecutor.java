/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql;

public interface SQLStatementExecutor {

    void executeNextStatement();

    boolean yieldIfNeeded(PreparedSQLStatement current);

    void wakeUp();

}
