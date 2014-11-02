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
package org.lealone.hbase.command.merge;

import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.Table;
import org.lealone.dbobject.table.TableFilter;
import org.lealone.engine.Session;
import org.lealone.hbase.dbobject.index.HBasePrimaryIndex;
import org.lealone.message.DbException;
import org.lealone.result.ResultInterface;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;

public class HBaseMergedIndex extends HBasePrimaryIndex {
    private final ResultInterface result;

    public HBaseMergedIndex(ResultInterface result, Table table, int id, IndexColumn[] columns, IndexType indexType) {
        super(table, id, columns, indexType);
        this.result = result;
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return new HBaseMergedCursor(result);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new HBaseMergedCursor(result);
    }

    @Override
    public void add(Session session, Row row) {
        throw DbException.throwInternalError();
    }

    @Override
    public void remove(Session session, Row row) {
        throw DbException.throwInternalError();
    }
}