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

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.Parameter;

class QueryResultCache {

    private final Select select;
    private final ServerSession session;

    boolean noCache;
    boolean useCache;
    private int lastLimit;
    private long lastEvaluated;
    protected LocalResult lastResult;
    private Value[] lastParameters;
    private boolean cacheableChecked;

    QueryResultCache(Select select) {
        this.select = select;
        session = select.getSession();
    }

    private Value[] getParameterValues() {
        ArrayList<Parameter> list = this.select.getParameters();
        if (list == null) {
            list = new ArrayList<>();
        }
        int size = list.size();
        Value[] params = new Value[size];
        for (int i = 0; i < size; i++) {
            Value v = list.get(i).getValue();
            params[i] = v;
        }
        return params;
    }

    private void closeLastResult() {
        if (lastResult != null) {
            lastResult.close();
        }
    }

    LocalResult getResult(int limit, ResultTarget target, boolean async) {
        if (noCache || !session.getDatabase().getOptimizeReuseResults()) {
            useCache = false;
            return queryWithoutCache(limit, target, async);
        } else {
            Value[] params = getParameterValues();
            long now = session.getDatabase().getModificationDataId();
            if (this.select.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)) {
                if (lastResult != null && !lastResult.isClosed() && limit == lastLimit) {
                    if (sameResultAsLast(session, params, lastParameters, lastEvaluated)) {
                        lastResult = lastResult.createShallowCopy(session);
                        if (lastResult != null) {
                            lastResult.reset();
                            useCache = true;
                            return lastResult;
                        }
                    } else {
                        useCache = false;
                    }
                }
            }
            lastParameters = params;
            closeLastResult();
            LocalResult r = queryWithoutCache(limit, target, async);
            if (!async) {
                lastResult = r;
            }
            this.lastEvaluated = now;
            lastLimit = limit;
            return r;
        }
    }

    private boolean sameResultAsLast(ServerSession s, Value[] params, Value[] lastParams, long lastEval) {
        if (!cacheableChecked) {
            long max = this.select.getMaxDataModificationId();
            noCache = max == Long.MAX_VALUE;
            cacheableChecked = true;
        }
        if (noCache) {
            return false;
        }
        Database db = s.getDatabase();
        for (int i = 0; i < params.length; i++) {
            Value a = lastParams[i], b = params[i];
            if (a.getType() != b.getType() || !db.areEqual(a, b)) {
                return false;
            }
        }
        if (!this.select.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)
                || !this.select.isEverything(ExpressionVisitor.INDEPENDENT_VISITOR)) {
            return false;
        }
        if (db.getModificationDataId() > lastEval && this.select.getMaxDataModificationId() > lastEval) {
            return false;
        }
        return true;
    }

    private LocalResult queryWithoutCache(int maxRows, ResultTarget target, boolean async) {
        // 按JDBC规范的要求，当调用java.sql.Statement.setMaxRows时，
        // 如果maxRows是0，表示不限制行数，相当于没有调用过setMaxRows一样，
        // 如果小余0，已经在客户端抛了无效参数异常，所以这里统一处理: 当limitRows小于0时表示不限制行数。
        int limitRows = maxRows == 0 ? -1 : maxRows;
        if (this.select.limitExpr != null) {
            // 如果在select语句中又指定了limit子句，那么用它覆盖maxRows
            // 注意limit 0表示不取任何记录，跟maxRows为0时刚好相反
            Value v = this.select.limitExpr.getValue(session);
            int l = v == ValueNull.INSTANCE ? -1 : v.getInt();
            if (limitRows < 0) {
                limitRows = l;
            } else if (l >= 0) {
                limitRows = Math.min(l, limitRows);
            }
        }
        int columnCount = this.select.expressions.size();
        LocalResult result = null;
        if (target == null || !session.getDatabase().getSettings().optimizeInsertFromSelect) {
            result = createLocalResult(result);
        }
        if (this.select.sort != null && (!this.select.sortUsingIndex || this.select.distinct)) {
            result = createLocalResult(result);
            result.setSortOrder(this.select.sort);
        }
        if (this.select.distinct && (!this.select.isDistinctQuery && !this.select.isDistinctQueryForMultiFields)) {
            result = createLocalResult(result);
            result.setDistinct();
        }
        if (this.select.randomAccessResult) {
            result = createLocalResult(result);
            // result.setRandomAccess(); //见H2的Mainly MVStore improvements的提交记录
        }
        if (this.select.isGroupQuery && !this.select.isGroupSortedQuery) {
            result = createLocalResult(result);
        }
        if (limitRows >= 0 || this.select.offsetExpr != null) {
            result = createLocalResult(result);
        }
        this.select.topTableFilter.startQuery(session);
        this.select.topTableFilter.reset();
        boolean exclusive = this.select.isForUpdate && !this.select.isForUpdateMvcc;
        if (this.select.isForUpdateMvcc) {
            if (this.select.isGroupQuery) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && GROUP");
            } else if (this.select.distinct) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && DISTINCT");
            } else if (this.select.isQuickAggregateQuery) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && AGGREGATE");
            } else if (this.select.topTableFilter.getJoin() != null) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && JOIN");
            }
        }
        this.select.topTableFilter.lock(session, exclusive);
        ResultTarget to = result != null ? result : target;
        if (limitRows != 0) {
            if (this.select.isQuickAggregateQuery) {
                this.select.queryOperator = new QQuick(this.select);
            } else if (this.select.isGroupQuery) {
                if (this.select.isGroupSortedQuery) {
                    this.select.queryOperator = new QGroupSorted(this.select);
                } else {
                    this.select.queryOperator = new QGroup(this.select);
                    to = result;
                }
            } else if (this.select.isDistinctQuery) {
                this.select.queryOperator = new QDistinct(this.select);
            } else if (this.select.isDistinctQueryForMultiFields) {
                this.select.queryOperator = new QDistinctForMultiFields(this.select);
            } else {
                this.select.queryOperator = new QFlat(this.select);
            }
        }
        this.select.queryOperator.columnCount = columnCount;
        this.select.queryOperator.maxRows = limitRows;
        this.select.queryOperator.target = target;
        this.select.queryOperator.result = to;
        this.select.queryOperator.localResult = result;
        this.select.queryOperator.async = async;
        this.select.queryOperator.start();
        if (!async) {
            this.select.queryOperator.run();
            this.select.queryOperator.stop();
            return this.select.queryOperator.localResult;
        }
        return null;
    }

    LocalResult createLocalResult(LocalResult old) {
        return old != null ? old
                : new LocalResult(session, this.select.expressionArray, this.select.visibleColumnCount);
    }
}
