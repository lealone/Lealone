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

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.session.SessionStatus;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.yieldable.YieldableQueryBase;

class YieldableSelect extends YieldableQueryBase {

    private final Select select;
    private final ResultTarget target;
    private QOperator queryOperator;

    public YieldableSelect(Select select, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler, ResultTarget target) {
        super(select, maxRows, scrollable, asyncHandler);
        this.select = select;
        this.target = target;
    }

    @Override
    protected boolean startInternal() {
        select.fireBeforeSelectTriggers();
        LocalResult result = select.resultCache.getResult(maxRows); // 不直接用limitRows
        int limitRows = getLimitRows(maxRows);
        if (result == null)
            queryOperator = createQueryOperator(limitRows, target);
        else
            queryOperator = new QCache(select, result, target);
        queryOperator.maxRows = limitRows;
        queryOperator.yieldableSelect = this;
        queryOperator.start();
        return false;
    }

    @Override
    protected void stopInternal() {
        queryOperator.stop();
    }

    @Override
    protected void executeInternal() {
        queryOperator.run();
        if (!queryOperator.loopEnd)
            return;
        // 查询结果已经增加到target了
        if (target != null) {
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        } else if (queryOperator.localResult != null) {
            setResult(queryOperator.localResult, queryOperator.localResult.getRowCount());
            select.resultCache.setResult(queryOperator.localResult);
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        }
    }

    private int getLimitRows(int maxRows) {
        // 按JDBC规范的要求，当调用java.sql.Statement.setMaxRows时，
        // 如果maxRows是0，表示不限制行数，相当于没有调用过setMaxRows一样，
        // 如果小余0，已经在客户端抛了无效参数异常，所以这里统一处理: 当limitRows小于0时表示不限制行数。
        int limitRows = maxRows == 0 ? -1 : maxRows;
        if (select.limitExpr != null) {
            // 如果在select语句中又指定了limit子句，那么用它覆盖maxRows
            // 注意limit 0表示不取任何记录，跟maxRows为0时刚好相反
            Value v = select.limitExpr.getValue(session);
            int l = v == ValueNull.INSTANCE ? -1 : v.getInt();
            if (limitRows < 0) {
                limitRows = l;
            } else if (l >= 0) {
                limitRows = Math.min(l, limitRows);
            }
        }
        return limitRows;
    }

    private QOperator createQueryOperator(int limitRows, ResultTarget target) {
        LocalResult result = null;
        if (target == null || !session.getDatabase().getSettings().optimizeInsertFromSelect) {
            result = createLocalResult(result);
        }
        if (select.sort != null && (!select.sortUsingIndex || select.distinct)) {
            result = createLocalResult(result);
            result.setSortOrder(select.sort);
        }
        if (select.distinct && (!select.isDistinctQuery && !select.isDistinctQueryForMultiFields)) {
            result = createLocalResult(result);
            result.setDistinct();
        }
        if (select.randomAccessResult) {
            result = createLocalResult(result);
            // result.setRandomAccess(); //见H2的Mainly MVStore improvements的提交记录
        }
        if (select.isGroupQuery && !select.isGroupSortedQuery) {
            result = createLocalResult(result);
        }
        if (limitRows >= 0 || select.offsetExpr != null) {
            result = createLocalResult(result);
        }
        select.topTableFilter.startQuery(session);
        select.topTableFilter.reset();
        boolean exclusive = select.isForUpdate && !select.isForUpdateMvcc;
        if (select.isForUpdateMvcc) {
            if (select.isGroupQuery) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && GROUP");
            } else if (select.distinct) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && DISTINCT");
            } else if (select.isQuickAggregateQuery) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && AGGREGATE");
            } else if (select.topTableFilter.getJoin() != null) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && JOIN");
            }
        }
        select.topTableFilter.lock(session, exclusive);
        ResultTarget to = result != null ? result : target;
        if (limitRows != 0) {
            if (select.isQuickAggregateQuery) {
                queryOperator = new QQuick(select);
            } else if (select.isGroupQuery) {
                if (select.isGroupSortedQuery) {
                    queryOperator = new QGroupSorted(select);
                } else {
                    queryOperator = new QGroup(select);
                    to = result;
                }
            } else if (select.isDistinctQuery) {
                queryOperator = new QDistinct(select);
            } else if (select.isDistinctQueryForMultiFields) {
                queryOperator = new QDistinctForMultiFields(select);
            } else {
                queryOperator = new QFlat(select);
            }
        } else {
            queryOperator = new QEmpty(select);
            result = createLocalResult(result);
        }
        queryOperator.columnCount = select.expressions.size();
        queryOperator.maxRows = limitRows;
        queryOperator.target = target;
        queryOperator.result = to;
        queryOperator.localResult = result;
        return queryOperator;
    }

    private LocalResult createLocalResult(LocalResult old) {
        return old != null ? old : new LocalResult(session, select.expressionArray, select.visibleColumnCount);
    }
}
