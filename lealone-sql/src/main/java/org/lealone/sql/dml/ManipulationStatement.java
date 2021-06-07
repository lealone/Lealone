/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.dml;

import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Expression;

public abstract class ManipulationStatement extends StatementBase {

    public ManipulationStatement(ServerSession session) {
        super(session);
    }

    static int getLimitRows(Expression limitExpr, ServerSession session) {
        int limitRows = -1;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            if (v != ValueNull.INSTANCE) {
                limitRows = v.getInt();
            }
        }
        return limitRows;
    }
}
