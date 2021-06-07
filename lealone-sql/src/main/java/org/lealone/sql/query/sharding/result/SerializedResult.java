/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding.result;

import java.util.List;

import org.lealone.db.result.DelegatedResult;
import org.lealone.db.result.Result;

public class SerializedResult extends DelegatedResult {

    private final static int UNKNOW_ROW_COUNT = -1;
    private final List<Result> results;
    private final int limitRows;

    private final int size;
    private int index = 0;
    private int count = 0;

    public SerializedResult(List<Result> results, int limitRows) {
        this.results = results;
        this.limitRows = limitRows;
        this.size = results.size();
        nextResult();
    }

    private boolean nextResult() {
        if (index >= size)
            return false;

        if (result != null)
            result.close();

        result = results.get(index++);
        return true;
    }

    @Override
    public boolean next() {
        count++;
        if (limitRows >= 0 && count > limitRows)
            return false;
        boolean next = result.next();
        if (!next) {
            boolean nextResult;
            while (true) {
                nextResult = nextResult();
                if (nextResult) {
                    next = result.next();
                    if (next)
                        return true;
                } else
                    return false;
            }
        }

        return next;
    }

    @Override
    public int getRowCount() {
        return UNKNOW_ROW_COUNT;
    }
}
