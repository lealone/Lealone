/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.lealone.hbase.command.merge;

import java.util.List;

import com.codefollower.lealone.command.dml.Select;
import com.codefollower.lealone.dbobject.index.IndexType;
import com.codefollower.lealone.dbobject.table.IndexColumn;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.hbase.result.HBaseSerializedResult;
import com.codefollower.lealone.result.DelegatedResult;
import com.codefollower.lealone.result.ResultInterface;

public class HBaseMergedResult extends DelegatedResult {
    public HBaseMergedResult(List<ResultInterface> results, Select newSelect, Select oldSelect) {
        //1. 结果集串行化，为合并做准备
        HBaseSerializedResult serializedResult = new HBaseSerializedResult(results);
        Table table = newSelect.getTopTableFilter().getTable();
        newSelect.getTopTableFilter().setIndex(
                new HBaseMergedIndex(serializedResult, table, -1, IndexColumn.wrap(table.getColumns()), IndexType
                        .createScan(false)));

        //2. 把多个结果集合并
        ResultInterface mergedResult = newSelect.queryGroupMerge();

        //3. 计算合并后的结果集,
        //例如oldSelect="select avg"时，在分布式环境要转成newSelect="select count, sum"，
        //此时就由count, sum来算出avg
        ResultInterface calculatedResult = oldSelect.calculate(mergedResult, newSelect);

        //4. 如果不存在avg、stddev这类需要拆分为count、sum的计算，此时mergedResult和calculatedResult是同一个实例
        //否则就是不同实例，需要再一次按oldSelect合并结果集
        if (mergedResult != calculatedResult) {
            table = oldSelect.getTopTableFilter().getTable();
            oldSelect.getTopTableFilter().setIndex(
                    new HBaseMergedIndex(calculatedResult, table, -1, IndexColumn.wrap(table.getColumns()), IndexType
                            .createScan(false)));
            //5. 最终结果集
            result = oldSelect.queryGroupMerge();

            //6. 立刻关闭中间结果集
            serializedResult.close();
            mergedResult.close();
            calculatedResult.close();
        } else {
            result = mergedResult;
        }

    }
}
