/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.evaluator;

import org.lealone.db.session.ServerSession;

public abstract class JitEvaluator implements ExpressionEvaluator {

    protected HotSpotEvaluator evaluator;
    protected ServerSession session;

    public void setHotSpotEvaluator(HotSpotEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    public void setSession(ServerSession session) {
        this.session = session;
    }
}
