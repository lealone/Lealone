/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.session.SessionStatus;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.operator.Operator;
import org.lealone.sql.operator.OperatorFactory;
import org.lealone.sql.operator.OperatorFactoryBase;
import org.lealone.sql.operator.OperatorFactoryManager;

public class YieldableSelect extends YieldableQueryBase {

    private final Select select;
    private final ResultTarget target;
    private Operator queryOperator;
    private boolean queryOperatorChanged;

    public YieldableSelect(Select select, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler, ResultTarget target) {
        super(select, maxRows, scrollable, asyncHandler);
        this.select = select;
        this.target = target;
    }

    public void setQueryOperator(Operator queryOperator) {
        this.queryOperator = queryOperator;
    }

    @Override
    public boolean yieldIfNeeded(int rowNumber) {
        if (!queryOperatorChanged && rowNumber > 1000) { // TODO 允许配置
            queryOperatorChanged = true;
            super.yieldIfNeeded(rowNumber);
            // createOlapOperatorAync();
            createOlapOperatorSync();
            return true;
        }
        return super.yieldIfNeeded(rowNumber);
    }

    // private void createOlapOperatorAync() {
    // Thread t = new Thread(() -> {
    // createOlapOperator();
    // });
    // t.setName("AsyncCreateOlapOperatorThread");
    // t.start();
    // }

    private void createOlapOperatorSync() {
        createOlapOperator();
    }

    private void createOlapOperator() {
        OperatorFactory operatorFactory = OperatorFactoryManager.getFactory("olap");
        if (operatorFactory == null) {
            operatorFactory = new VOperatorFactory();
        }
        if (operatorFactory != null) {
            Operator olapOperator = operatorFactory.createOperator(select, queryOperator.getLocalResult());
            olapOperator.start();
            setQueryOperator(olapOperator);
        }
    }

    @Override
    protected boolean startInternal() {
        select.topTableFilter.lock(session, select.isForUpdate);
        select.topTableFilter.startQuery(session);
        select.topTableFilter.reset();
        select.fireBeforeSelectTriggers();
        queryOperator = new OltpOperatorFactory().createOperator(select);
        queryOperator.start();
        return false;
    }

    @Override
    protected void stopInternal() {
        // 执行startInternal抛异常时queryOperator可能为null
        if (queryOperator != null)
            queryOperator.stop();
    }

    @Override
    protected void executeInternal() {
        queryOperator.run();
        if (queryOperator.isStopped()) {
            // 查询结果已经增加到target了
            if (target != null) {
                session.setStatus(SessionStatus.STATEMENT_COMPLETED);
            } else if (queryOperator.getLocalResult() != null) {
                setResult(queryOperator.getLocalResult(), queryOperator.getLocalResult().getRowCount());
                select.resultCache.setResult(queryOperator.getLocalResult());
                session.setStatus(SessionStatus.STATEMENT_COMPLETED);
            }
        }
    }

    private class OltpOperatorFactory extends OperatorFactoryBase {

        public OltpOperatorFactory() {
            super("oltp");
        }

        @Override
        public Operator createOperator(Select select) {
            return createQueryOperator();
        }
    }

    private QOperator createQueryOperator() {
        LocalResult result;
        ResultTarget to;
        QOperator queryOperator;
        int limitRows = getLimitRows(maxRows);
        LocalResult cachedResult = select.resultCache.getResult(maxRows); // 不直接用limitRows
        if (cachedResult != null) {
            result = cachedResult;
            to = cachedResult;
            queryOperator = new QCache(select, cachedResult);
        } else {
            result = createLocalResultIfNeeded(limitRows);
            to = result != null ? result : target;
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
            }
        }
        queryOperator.columnCount = select.expressions.size();
        queryOperator.maxRows = limitRows;
        queryOperator.target = target;
        queryOperator.result = to;
        queryOperator.localResult = result;
        queryOperator.yieldableSelect = this;
        return queryOperator;
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

    private LocalResult createLocalResultIfNeeded(int limitRows) {
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
        return result;
    }

    private LocalResult createLocalResult(LocalResult old) {
        return old != null ? old : new LocalResult(session, select.expressionArray, select.visibleColumnCount);
    }
}
