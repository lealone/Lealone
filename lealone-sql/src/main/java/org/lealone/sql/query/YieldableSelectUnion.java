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
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.yieldable.YieldableBase;
import org.lealone.sql.yieldable.YieldableQueryBase;

class YieldableSelectUnion extends YieldableQueryBase {

    private final SelectUnion selectUnion;
    private final ResultTarget target;

    Expression limitExpr;
    boolean insertFromSelect;
    YieldableBase<Result> leftYieldableQuery;
    YieldableBase<Result> rightYieldableQuery;
    int columnCount;
    LocalResult result;
    Result leftRows;
    Result rightRows;
    LocalResult temp;
    int rowNumber;
    boolean done;

    public YieldableSelectUnion(SelectUnion selectUnion, SelectUnion statement, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler, ResultTarget target) {
        super(statement, maxRows, scrollable, asyncHandler);
        this.selectUnion = selectUnion;
        this.target = target;
    }

    @Override
    protected boolean startInternal() {
        this.selectUnion.fireBeforeSelectTriggers();
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
            if (this.selectUnion.unionType == SelectUnion.UNION_ALL && target != null) {
                if (this.selectUnion.sort == null && !this.selectUnion.distinct && maxRows == 0
                        && this.selectUnion.offsetExpr == null && limitExpr == null) {
                    insertFromSelect = true;
                    leftYieldableQuery = this.selectUnion.left.createYieldableQuery(0, false, null, target);
                    rightYieldableQuery = this.selectUnion.right.createYieldableQuery(0, false, null, target);
                    return false;
                }
            }
        }
        columnCount = this.selectUnion.left.getColumnCount();
        result = new LocalResult(session, this.selectUnion.expressionArray, columnCount);
        if (this.selectUnion.sort != null) {
            result.setSortOrder(this.selectUnion.sort);
        }
        if (this.selectUnion.distinct) {
            this.selectUnion.left.setDistinct(true);
            this.selectUnion.right.setDistinct(true);
            result.setDistinct();
        }
        if (this.selectUnion.randomAccessResult) {
            result.setRandomAccess();
        }
        switch (this.selectUnion.unionType) {
        case SelectUnion.UNION:
        case SelectUnion.EXCEPT:
            this.selectUnion.left.setDistinct(true);
            this.selectUnion.right.setDistinct(true);
            result.setDistinct();
            break;
        case SelectUnion.UNION_ALL:
            break;
        case SelectUnion.INTERSECT:
            this.selectUnion.left.setDistinct(true);
            this.selectUnion.right.setDistinct(true);
            temp = new LocalResult(session, this.selectUnion.expressionArray, columnCount);
            temp.setDistinct();
            temp.setRandomAccess();
            break;
        default:
            DbException.throwInternalError("type=" + this.selectUnion.unionType);
        }
        leftYieldableQuery = this.selectUnion.left.createYieldableQuery(0, false, null, null);
        rightYieldableQuery = this.selectUnion.right.createYieldableQuery(0, false, null, null);
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

        switch (this.selectUnion.unionType) {
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
                    result.removeDistinct(this.selectUnion.convert(rightRows.currentRow(), columnCount));
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
                    temp.addRow(this.selectUnion.convert(leftRows.currentRow(), columnCount));
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
                    Value[] values = this.selectUnion.convert(rightRows.currentRow(), columnCount);
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
            DbException.throwInternalError("type=" + this.selectUnion.unionType);
        }
        if (!done) {
            if (this.selectUnion.offsetExpr != null) {
                result.setOffset(this.selectUnion.offsetExpr.getValue(session).getInt());
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
            result.addRow(this.selectUnion.convert(leftRows.currentRow(), columnCount));
            if (yieldIfNeeded(++rowNumber)) {
                return true;
            }
        }
        leftRows = null;
        return false;
    }

    private boolean addRightRows() {
        while (rightRows.next()) {
            result.addRow(this.selectUnion.convert(rightRows.currentRow(), columnCount));
            if (yieldIfNeeded(++rowNumber)) {
                return true;
            }
        }
        rightRows = null;
        return false;
    }
}
