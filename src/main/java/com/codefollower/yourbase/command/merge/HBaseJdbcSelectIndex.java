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
package com.codefollower.yourbase.command.merge;

import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.result.ResultInterface;
import org.h2.result.SearchRow;
import org.h2.table.Table;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

import com.codefollower.yourbase.index.HBaseTableIndex;

public class HBaseJdbcSelectIndex extends HBaseTableIndex {
    private final ResultInterface result;

    public HBaseJdbcSelectIndex(ResultInterface result, Table table, int id, IndexColumn[] columns, IndexType indexType) {
        super(table, id, columns, indexType);
        this.result = result;
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return new HBaseJdbcSelectCursor(result);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new HBaseJdbcSelectCursor(result);
    }
}