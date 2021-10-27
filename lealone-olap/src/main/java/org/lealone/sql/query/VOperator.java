/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.ArrayList;

import org.lealone.db.result.Row;

public class VOperator extends QOperator {

    protected ArrayList<Row> batch;

    VOperator(Select select) {
        super(select);
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
