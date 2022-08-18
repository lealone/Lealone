/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.executor.YieldableBase;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ValueExpression;

class YieldableSelectUnion extends YieldableQueryBase {

    private final SelectUnion selectUnion;
    private final ResultTarget target;

    private Expression limitExpr;
    private boolean insertFromSelect;
    private YieldableBase<Result> leftYieldableQuery;
    private YieldableBase<Result> rightYieldableQuery;
    private int columnCount;
    private LocalResult result;
    private Result leftRows;
    private Result rightRows;
    private LocalResult temp;
    private int rowNumber;
    private boolean done;

    public YieldableSelectUnion(SelectUnion selectUnion, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler, ResultTarget target) {
        super(selectUnion, maxRows, scrollable, asyncHandler);
        this.selectUnion = selectUnion;
        this.target = target;
    }

    @Override
    protected boolean startInternal() {
        selectUnion.fireBeforeSelectTriggers();
        // union doesn't always know the parameter list of the left and right queries
        if (maxRows != 0) {
            // maxRows is set (maxRows 0 means no limit)
            int l;
            if (limitExpr == null) {
                l = -1;
            } else {
                Value v = limitExpr.getValue(session);
                l = v == ValueNull.INSTANCE ? -1 : v.getInt();
            }
            if (l < 0) {
                // for limitExpr, 0 means no rows, and -1 means no limit
                l = maxRows;
            } else {
                l = Math.min(l, maxRows);
            }
            limitExpr = ValueExpression.get(ValueInt.get(l));
        }

        if (session.getDatabase().getSettings().optimizeInsertFromSelect) {
            if (selectUnion.unionType == SelectUnion.UNION_ALL && target != null) {
                if (selectUnion.sort == null && !selectUnion.distinct && maxRows == 0
                        && selectUnion.offsetExpr == null && limitExpr == null) {
                    insertFromSelect = true;
                    leftYieldableQuery = selectUnion.left.createYieldableQuery(0, false, null, target);
                    rightYieldableQuery = selectUnion.right.createYieldableQuery(0, false, null, target);
                    return false;
                }
            }
        }
        columnCount = selectUnion.left.getColumnCount();
        result = new LocalResult(session, selectUnion.expressionArray, columnCount);
        if (selectUnion.sort != null) {
            result.setSortOrder(selectUnion.sort);
        }
        if (selectUnion.distinct) {
            selectUnion.left.setDistinct(true);
            selectUnion.right.setDistinct(true);
            result.setDistinct();
        }
        if (selectUnion.randomAccessResult) {
            result.setRandomAccess();
        }
        switch (selectUnion.unionType) {
        case SelectUnion.UNION:
        case SelectUnion.EXCEPT:
            selectUnion.left.setDistinct(true);
            selectUnion.right.setDistinct(true);
            result.setDistinct();
            break;
        case SelectUnion.UNION_ALL:
            break;
        case SelectUnion.INTERSECT:
            selectUnion.left.setDistinct(true);
            selectUnion.right.setDistinct(true);
            temp = new LocalResult(session, selectUnion.expressionArray, columnCount);
            temp.setDistinct();
            temp.setRandomAccess();
            break;
        default:
            DbException.throwInternalError("type=" + selectUnion.unionType);
        }
        leftYieldableQuery = selectUnion.left.createYieldableQuery(0, false, null, null);
        rightYieldableQuery = selectUnion.right.createYieldableQuery(0, false, null, null);
        return false;
    }

    @Override
    protected void stopInternal() {
    }

    @Override
    protected void executeInternal() {
        if (insertFromSelect) {
            if (leftYieldableQuery != null) {
                leftYieldableQuery.run();
                if (leftYieldableQuery.isStopped()) {
                    leftYieldableQuery = null;
                }
            }
            if (leftYieldableQuery == null && rightYieldableQuery != null) {
                rightYieldableQuery.run();
                if (rightYieldableQuery.isStopped()) {
                    rightYieldableQuery = null;
                }
            }
        }

        switch (selectUnion.unionType) {
        case SelectUnion.UNION_ALL:
        case SelectUnion.UNION: {
            if (leftYieldableQuery != null && runLeftQuery()) {
                return;
            }
            if (leftRows != null && addLeftRows()) {
                return;
            }

            if (rightYieldableQuery != null && runRightQuery()) {
                return;
            }
            if (rightRows != null && addRightRows()) {
                return;
            }
            break;
        }
        case SelectUnion.EXCEPT: {
            if (leftYieldableQuery != null && runLeftQuery()) {
                return;
            }
            if (leftRows != null && addLeftRows()) {
                return;
            }

            if (rightYieldableQuery != null && runRightQuery()) {
                return;
            }
            if (rightRows != null) {
                while (rightRows.next()) {
                    result.removeDistinct(convert(rightRows.currentRow(), columnCount));
                    if (yieldIfNeeded(++rowNumber)) {
                        return;
                    }
                }
                rightRows = null;
            }
            break;
        }
        case SelectUnion.INTERSECT: {
            if (leftYieldableQuery != null && runLeftQuery()) {
                return;
            }
            if (leftRows != null) {
                while (leftRows.next()) {
                    temp.addRow(convert(leftRows.currentRow(), columnCount));
                    if (yieldIfNeeded(++rowNumber)) {
                        return;
                    }
                }
                leftRows = null;
            }

            if (rightYieldableQuery != null && runRightQuery()) {
                return;
            }
            if (rightRows != null) {
                while (rightRows.next()) {
                    Value[] values = convert(rightRows.currentRow(), columnCount);
                    if (temp.containsDistinct(values)) {
                        result.addRow(values);
                    }
                    if (yieldIfNeeded(++rowNumber)) {
                        return;
                    }
                }
                rightRows = null;
            }
            break;
        }
        default:
            DbException.throwInternalError("type=" + selectUnion.unionType);
        }
        if (!done) {
            if (selectUnion.offsetExpr != null) {
                result.setOffset(selectUnion.offsetExpr.getValue(session).getInt());
            }
            if (limitExpr != null) {
                Value v = limitExpr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    result.setLimit(v.getInt());
                }
            }
            rowNumber = 0;
            result.done();
            done = true;
        }
        if (target == null) {
            setResult(result, result.getRowCount());
        } else {
            while (result.next()) {
                target.addRow(result.currentRow());
                if (yieldIfNeeded(++rowNumber)) {
                    return;
                }
            }
            result.close();
        }
        session.setStatus(SessionStatus.STATEMENT_COMPLETED);
    }

    private boolean runLeftQuery() {
        if (leftRows == null) {
            leftYieldableQuery.run();
            if (leftYieldableQuery.isStopped()) {
                rowNumber = 0;
                leftRows = leftYieldableQuery.getResult();
                leftYieldableQuery = null;
                return false;
            }
        }
        return true;
    }

    private boolean runRightQuery() {
        if (rightRows == null) {
            rightYieldableQuery.run();
            if (rightYieldableQuery.isStopped()) {
                rowNumber = 0;
                rightRows = rightYieldableQuery.getResult();
                rightYieldableQuery = null;
                return false;
            }
        }
        return true;
    }

    private boolean addLeftRows() {
        while (leftRows.next()) {
            result.addRow(convert(leftRows.currentRow(), columnCount));
            if (yieldIfNeeded(++rowNumber)) {
                return true;
            }
        }
        leftRows = null;
        return false;
    }

    private boolean addRightRows() {
        while (rightRows.next()) {
            result.addRow(convert(rightRows.currentRow(), columnCount));
            if (yieldIfNeeded(++rowNumber)) {
                return true;
            }
        }
        rightRows = null;
        return false;
    }

    private Value[] convert(Value[] values, int columnCount) {
        Value[] newValues;
        if (columnCount == values.length) {
            // re-use the array if possible
            newValues = values;
        } else {
            // create a new array if needed,
            // for the value hash set
            newValues = new Value[columnCount];
        }
        for (int i = 0; i < columnCount; i++) {
            Expression e = selectUnion.expressions.get(i);
            newValues[i] = values[i].convertTo(e.getType());
        }
        return newValues;
    }
}
