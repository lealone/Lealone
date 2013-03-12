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
package com.codefollower.lealone.hbase.command.dml;

import com.codefollower.lealone.command.dml.Delete;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.command.HBasePrepared;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.value.Value;

//TODO
//目前还不支持按family、qualifier、timestamp删除
//可删除多条记录，但是因为不支持事务，所以有可能出现部分删除成功、部分删除失败。
public class HBaseDelete extends Delete implements HBasePrepared {
    private String regionName;

    public HBaseDelete(Session session) {
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
    public String getTableName() {
        return tableFilter.getTable().getName();
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
