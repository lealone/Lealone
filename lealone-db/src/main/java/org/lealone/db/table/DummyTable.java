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
package org.lealone.db.table;

import java.util.ArrayList;

import org.lealone.db.index.Index;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;

public class DummyTable extends Table {

    public DummyTable(Schema schema, String name) {
        super(schema, -1, name, false, false);
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public TableType getTableType() {
        return null;
    }

    @Override
    public Index getScanIndex(ServerSession session) {
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
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
    public boolean canDrop() {
        return false;
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public long getRowCount(ServerSession session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

}
