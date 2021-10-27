/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.ArrayList;

import org.lealone.db.result.Row;
import org.lealone.sql.operator.Operator;

public class VOperator extends QOperator {

    protected ArrayList<Row> batch;

    VOperator(Select select) {
        super(select);
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
        }
    }

    public boolean nextBatch() {
        int size = 1024;
        batch = new ArrayList<>(size);
        while (size-- > 0 && select.topTableFilter.next()) {
            batch.add(select.topTableFilter.get());
        }
        return !batch.isEmpty();
    }
}
