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

import com.codefollower.yourbase.command.dml.Update;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.hbase.command.HBasePrepared;
import com.codefollower.yourbase.result.SearchRow;
import com.codefollower.yourbase.value.Value;

//TODO
//可Update多条记录，但是因为不支持事务，所以有可能出现部分Update成功、部分Update失败。
public class HBaseUpdate extends Update implements HBasePrepared {
    private String regionName;

    public HBaseUpdate(Session session) {
        super(session);
    }

    @Override
    public void prepare() {
        super.prepare();
        tableFilter.setPrepared(this);
    }

    @Override
    public boolean isDistributedSQL() {
        return true;
    }

    @Override
    public String getRowKey() {
        Value rowKey = getStartRowKeyValue();
        if (rowKey != null)
            rowKey.getString();
        return null;
    }

    @Override
    public String getTableName() {
        return tableFilter.getTable().getName();
    }

    @Override
    public Value getStartRowKeyValue() {
        SearchRow start = tableFilter.getStartSearchRow();
        if (start != null)
            return start.getRowKey();
        return null;
    }

    @Override
    public Value getEndRowKeyValue() {
        SearchRow end = tableFilter.getEndSearchRow();
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
