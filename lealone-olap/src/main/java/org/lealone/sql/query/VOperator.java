/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.ArrayList;

import org.lealone.db.result.Row;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.evaluator.ExpressionEvaluator;
import org.lealone.sql.expression.evaluator.HotSpotEvaluator;
import org.lealone.sql.expression.visitor.GetValueVectorVisitor;
import org.lealone.sql.operator.Operator;
import org.lealone.sql.vector.ValueVector;

public class VOperator extends QOperator {

    protected static final int MAX_BATCH_SIZE = 1024;

    protected ArrayList<Row> batch = new ArrayList<>();

    VOperator(Select select) {
        super(select);
    }

    @Override
    ExpressionEvaluator createConditionEvaluator(Expression c) {
        return new HotSpotEvaluator(session, c);
    }

    @Override
    public void copyStatus(Operator old) {
        if (old instanceof QOperator) {
            QOperator q = (QOperator) old;
            columnCount = q.columnCount;
            target = q.target;
            result = q.result;
            localResult = q.localResult;
            maxRows = q.maxRows;
            limitRows = q.limitRows;
            sampleSize = q.sampleSize;
            rowCount = q.rowCount;
            loopCount = q.loopCount;
            yieldableSelect = q.yieldableSelect;

            rowCount++; // 调用YieldableSelect.yieldIfNeeded后，oltp转olap之前，QOperator的子类还会执行一次
        }
    }

    protected boolean nextBatch() {
        batch.clear();
        for (int i = 0; select.topTableFilter.next() && i < MAX_BATCH_SIZE; i++) {
            batch.add(select.topTableFilter.get());
        }
        return !batch.isEmpty();
    }

    protected ValueVector getConditionValueVector() {
        ValueVector conditionValueVector = null;
        if (select.condition != null) {
            GetValueVectorVisitor visitor = new GetValueVectorVisitor(session, null, batch);
            conditionValueVector = select.condition.accept(visitor);
        }
        return conditionValueVector;
    }

    protected int getBatchSize(ValueVector conditionValueVector) {
        return conditionValueVector == null ? batch.size() : conditionValueVector.trueCount();
    }
}
