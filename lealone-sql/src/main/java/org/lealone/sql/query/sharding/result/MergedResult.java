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
        newSelect.getTopTableFilter().setIndex(new MergedIndex(serializedResult, table, -1,
                IndexType.createScan(), IndexColumn.wrap(table.getColumns())));

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
