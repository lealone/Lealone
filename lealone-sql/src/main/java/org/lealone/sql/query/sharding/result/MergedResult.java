/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding.result;

import java.util.List;

import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.DelegatedResult;
import org.lealone.db.result.Result;
import org.lealone.db.table.Table;
import org.lealone.sql.query.Select;

public class MergedResult extends DelegatedResult {

    public MergedResult(List<Result> results, Select newSelect, Select oldSelect) {
        // 1. 结果集串行化，为合并做准备
        SerializedResult serializedResult = new SerializedResult(results, oldSelect.getLimitRows());
        Table table = newSelect.getTopTableFilter().getTable();
        newSelect.getTopTableFilter().setIndex(new MergedIndex(serializedResult, table, -1, IndexType.createScan(),
                IndexColumn.wrap(table.getColumns())));

        // 2. 把多个结果集合并
        Result mergedResult = newSelect.queryGroupMerge();

        // 3. 计算合并后的结果集,
        // 例如oldSelect="select avg"时，在分布式环境要转成newSelect="select count, sum"，
        // 此时就由count, sum来算出avg
        Result calculatedResult = oldSelect.calculate(mergedResult, newSelect);

        // 4. 如果不存在avg、stddev这类需要拆分为count、sum的计算，
        // 此时mergedResult和calculatedResult是同一个实例，否则就是不同实例
        if (mergedResult != calculatedResult) {
            mergedResult.close();
            result = calculatedResult;
        } else {
            result = mergedResult;
        }
    }
}
