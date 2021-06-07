/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding;

import org.lealone.db.result.Result;
import org.lealone.sql.query.sharding.result.SerializedResult;

//表示在sharding场景下，不含聚合、分组、排序的查询语句，
//多个节点返回的结果会串行化返回给客户端
public class SQSerialize extends SQOperator {

    private final int limitRows;

    public SQSerialize(SQCommand[] commands, int maxRows, int limitRows) {
        super(commands, maxRows);
        this.limitRows = limitRows;
    }

    @Override
    protected Result createFinalResult() {
        return new SerializedResult(results, limitRows);
    }
}
