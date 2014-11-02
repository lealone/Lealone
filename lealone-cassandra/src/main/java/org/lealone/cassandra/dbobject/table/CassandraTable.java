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
package org.lealone.cassandra.dbobject.table;

import java.util.ArrayList;

import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.index.Index;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.TableBase;
import org.lealone.engine.Session;

public class CassandraTable extends TableBase {

    public CassandraTable(CreateTableData data) {
        super(data);
    }

    @Override
    public void lock(Session session, boolean exclusive, boolean force) {

    }

    @Override
    public void close(Session session) {

    }

    @Override
    public void unlock(Session s) {

    }

    @Override
    public void truncate(Session session) {

    }

    @Override
    public void checkSupportAlter() {

    }

    @Override
    public String getTableType() {
        return "CASSANDRA TABLE";
    }

    @Override
    public Index getScanIndex(Session session) {
        return scanIndex;
    }

    @Override
    public Index getUniqueIndex() {
        return scanIndex;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public boolean isLockedExclusively() {
        return false;
    }

    @Override
    public long getMaxDataModificationId() {
        return 0;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public long getRowCount(Session session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

    @Override
    public boolean isDistributed() {
        return true;
    }

    @Override
    public boolean supportsColumnFamily() {
        return true;
    }

    @Override
    public boolean supportsAlterColumnWithCopyData() {
        return false;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {
        return null;
    }

}
