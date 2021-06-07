/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding;

import org.lealone.sql.StatementBase;

//直接查当前结点
public class SQDirect extends SQOperator {

    private final StatementBase statement;

    public SQDirect(StatementBase statement, int maxRows) {
        super(null, maxRows);
        this.statement = statement;
    }

    @Override
    public void run() {
        result = statement.query(maxRows);
        end = true;
    }
}
