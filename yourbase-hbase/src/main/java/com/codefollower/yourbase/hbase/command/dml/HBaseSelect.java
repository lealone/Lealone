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
package com.codefollower.yourbase.hbase.command.dml;

import com.codefollower.yourbase.command.dml.Select;
import com.codefollower.yourbase.dbobject.table.TableView;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.hbase.command.HBasePrepared;
import com.codefollower.yourbase.result.SearchRow;
import com.codefollower.yourbase.value.Value;

public class HBaseSelect extends Select implements HBasePrepared {
    private String regionName;

    public HBaseSelect(Session session) {
        super(session);
    }

    @Override
    public boolean isDistributedSQL() {
        if (topTableFilter.getTable().isDistributed())
            return true;
        return super.isDistributedSQL();
    }

    @Override
    public String getTableName() {
        if ((topTableFilter.getTable() instanceof TableView))
            return ((TableView) topTableFilter.getTable()).getTableName();
        else
            return topTableFilter.getTable().getName();
    }

    @Override
    public String getRowKey() {
        Value rowKey = getStartRowKeyValue();
        if (rowKey != null)
            rowKey.getString();
        return null;
    }

    @Override
    public Value getStartRowKeyValue() {
        SearchRow start = topTableFilter.getStartSearchRow();
        if (start != null)
            return start.getRowKey();
        return null;
    }

    @Override
    public Value getEndRowKeyValue() {
        SearchRow end = topTableFilter.getEndSearchRow();
        if (end != null)
            return end.getRowKey();
        return null;
    }

    @Override
    public String getRegionName() {
        return regionName;
    }

    @Override
    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

}
