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
package org.lealone.sql.query;

import java.util.ArrayList;

import org.lealone.db.Database;
import org.lealone.db.result.LocalResult;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.Parameter;

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
            if (lastResult != null && !lastResult.isClosed() && limit == lastLimit
                    && select.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)) {
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
        if (!select.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)
                || !select.isEverything(ExpressionVisitor.INDEPENDENT_VISITOR)) {
            return false;
        }
        if (db.getModificationDataId() > lastEvaluated && select.getMaxDataModificationId() > lastEvaluated) {
            return false;
        }
        return true;
    }
}
