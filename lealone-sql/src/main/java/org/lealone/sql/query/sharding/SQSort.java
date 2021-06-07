/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding;

import org.lealone.db.result.Result;
import org.lealone.sql.query.Select;
import org.lealone.sql.query.sharding.result.SortedResult;

//表示在sharding场景下，不含聚合、分组但是带排序的查询语句，
//多个节点返回的已排序的结果对它们一边进行排序一边返回给客户端
public class SQSort extends SQOperator {

    private final Select select;

    public SQSort(SQCommand[] commands, int maxRows, Select select) {
        super(commands, maxRows);
        this.select = select;
    }

    @Override
    protected Result createFinalResult() {
        return new SortedResult(select, results, maxRows);
    }
}
