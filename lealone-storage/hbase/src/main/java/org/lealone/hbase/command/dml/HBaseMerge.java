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
package org.lealone.hbase.command.dml;

import org.lealone.command.dml.Merge;
import org.lealone.engine.Session;
import org.lealone.expression.Expression;
import org.lealone.result.Row;
import org.lealone.value.Value;

public class HBaseMerge extends Merge implements WithRegionNames {
    private final InsertOrMergeSupport insertOrMergeSupport;

    public HBaseMerge(Session session) {
        super(session);
        insertOrMergeSupport = new InsertOrMergeSupport(session, this, false);
    }

    @Override
    public void prepare() {
        super.prepare();
        if (table.supportsSharding())
            insertOrMergeSupport.prepare(table, query, list, columns, keys);
        else
            setLocal(true);
    }

    @Override
    public int update() {
        createRows();
        if (isLocal())
            return updateLocal();
        return insertOrMergeSupport.update(false, false, this);
    }

    @Override
    protected Row createRow(Expression[] expr, int rowId) {
        return insertOrMergeSupport.createRow(expr, rowId);
    }

    @Override
    protected Row createRow(Value[] values) {
        return insertOrMergeSupport.createRow(values);
    }

    @Override
    public String[] getLocalRegionNames() {
        return insertOrMergeSupport.getLocalRegionNames();
    }

    @Override
    public void setLocalRegionNames(String[] localRegionNames) {
        insertOrMergeSupport.setLocalRegionNames(localRegionNames);
    }
}
