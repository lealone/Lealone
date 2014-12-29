package org.lealone.postgresql.dbobject.table;

import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.index.Index;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.TableBase;
import org.lealone.engine.Session;

public class PostgreSQLTable extends TableBase {

    public PostgreSQLTable(CreateTableData data) {
        super(data);
        // TODO Auto-generated constructor stub
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void unlock(Session s) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isLockedExclusively() {
        // TODO Auto-generated method stub
        return false;
    }

}
