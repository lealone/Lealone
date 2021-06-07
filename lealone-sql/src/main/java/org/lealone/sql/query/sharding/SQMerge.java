/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding;

import org.lealone.db.result.Result;
import org.lealone.sql.query.Select;
import org.lealone.sql.query.sharding.result.MergedResult;

//表示在sharding场景下，包含聚合、分组的查询语句，
//多个节点返回的结果会再次进行聚合、分组合并后再返回给客户端
public class SQMerge extends SQOperator {

    private final Select oldSelect;
    private final Select newSelect;

    public SQMerge(SQCommand[] commands, int maxRows, Select oldSelect) {
        super(commands, maxRows);
        this.oldSelect = oldSelect;
        String newSQL = oldSelect.getPlanSQL(true, true);
        newSelect = (Select) oldSelect.getSession().prepareStatement(newSQL, true);
        newSelect.setLocal(true);
    }

    @Override
    protected Result createFinalResult() {
        return new MergedResult(results, newSelect, oldSelect);
    }
}
