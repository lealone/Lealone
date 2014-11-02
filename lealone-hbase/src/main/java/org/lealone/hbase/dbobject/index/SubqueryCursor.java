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
package org.lealone.hbase.dbobject.index;

import org.lealone.command.Prepared;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.table.TableFilter;
import org.lealone.expression.Expression;
import org.lealone.hbase.result.HBaseRow;
import org.lealone.result.ResultInterface;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.util.StringUtils;

public class SubqueryCursor implements Cursor {
    private final ResultInterface subqueryResult;

    public SubqueryCursor(TableFilter filter, SearchRow first, SearchRow last) {
        StringBuilder buff = new StringBuilder("SELECT * FROM ");
        buff.append(filter.getTable().getSQL());
        Expression filterCondition = filter.getFilterCondition();
        if (filterCondition != null) {
            buff.append(" WHERE ").append(StringUtils.unEnclose(filterCondition.getSQL()));
        }
        Prepared prepared = filter.getSession().prepare(buff.toString(), true);
        subqueryResult = prepared.query(-1);
    }

    @Override
    public Row get() {
        return new HBaseRow(subqueryResult.currentRow(), Row.MEMORY_CALCULATE);
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        return subqueryResult.next();
    }

    @Override
    public boolean previous() {
        return false;
    }

}
