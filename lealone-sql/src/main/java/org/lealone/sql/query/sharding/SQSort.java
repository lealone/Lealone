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
