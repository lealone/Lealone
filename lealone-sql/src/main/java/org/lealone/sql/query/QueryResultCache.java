/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.ArrayList;

import org.lealone.db.Database;
import org.lealone.db.result.LocalResult;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.visitor.ExpressionVisitorFactory;

class QueryResultCache {

    private final Select select;
    private final ServerSession session;

    private boolean noCache;
    private int lastLimit;
    private long lastEvaluated;
    private Value[] lastParameters;
    private LocalResult lastResult;
    private boolean cacheableChecked;

    QueryResultCache(Select select) {
        this.select = select;
        session = select.getSession();
    }

    void disable() {
        noCache = true;
    }

    void setResult(LocalResult r) {
        lastResult = r;
    }

    LocalResult getResult(int limit) {
        if (noCache || !session.getDatabase().getOptimizeReuseResults()) {
            return null;
        } else {
            Value[] params = getParameterValues();
            long now = session.getDatabase().getModificationDataId();
            // 当lastEvaluated != now时，说明数据已经有变化，缓存的结果不能用了
            if (lastEvaluated == now && lastResult != null && !lastResult.isClosed()
                    && limit == lastLimit
                    && select.accept(ExpressionVisitorFactory.getDeterministicVisitor())) {
                if (sameResultAsLast(params)) {
                    lastResult = lastResult.createShallowCopy(session);
                    if (lastResult != null) {
                        lastResult.reset();
                        return lastResult;
                    }
                }
            }
            lastLimit = limit;
            lastEvaluated = now;
            lastParameters = params;
            if (lastResult != null) {
                lastResult.close();
                lastResult = null;
            }
            return null;
        }
    }

    private Value[] getParameterValues() {
        ArrayList<Parameter> list = select.getParameters();
        if (list == null || list.isEmpty()) {
            return new Value[0];
        }
        int size = list.size();
        Value[] params = new Value[size];
        for (int i = 0; i < size; i++) {
            params[i] = list.get(i).getValue();
        }
        return params;
    }

    private boolean sameResultAsLast(Value[] params) {
        if (!cacheableChecked) {
            long max = select.getMaxDataModificationId();
            noCache = max == Long.MAX_VALUE;
            cacheableChecked = true;
        }
        if (noCache) {
            return false;
        }
        Database db = session.getDatabase();
        for (int i = 0; i < params.length; i++) {
            Value a = lastParameters[i], b = params[i];
            if (a.getType() != b.getType() || !db.areEqual(a, b)) {
                return false;
            }
        }
        if (!select.accept(ExpressionVisitorFactory.getDeterministicVisitor())
                || !select.accept(ExpressionVisitorFactory.getIndependentVisitor())) {
            return false;
        }
        if (db.getModificationDataId() > lastEvaluated
                && select.getMaxDataModificationId() > lastEvaluated) {
            return false;
        }
        return true;
    }
}
