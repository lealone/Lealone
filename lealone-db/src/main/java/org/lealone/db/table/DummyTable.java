/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.table;

import org.lealone.db.index.Index;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;

//只用于创建服务时的void类型解析
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
        return TableType.DUMMY_TABLE;
    }

    @Override
    public Index getScanIndex(ServerSession session) {
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
