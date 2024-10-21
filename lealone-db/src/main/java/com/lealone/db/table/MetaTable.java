/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.table;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.index.Index;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.MetaIndex;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueString;

/**
 * This class is responsible to build the database meta data pseudo tables.
 *
 * @author H2 Group
 * @author zhh
 */
public abstract class MetaTable extends Table {

    /**
     * The approximate number of rows of a meta table.
     */
    public static final long ROW_COUNT_APPROXIMATION = 1000;

    protected final int type;
    protected final int indexColumn;
    protected final MetaIndex metaIndex;

    /**
     * Create a new metadata table.
     *
     * @param schema the schema
     * @param id the object id
     * @param type the meta table type
     */
    public MetaTable(Schema schema, int id, int type) {
        // tableName will be set later
        super(schema, id, null, true, true);
        this.type = type;
        String indexColumnName = createColumns();
        if (indexColumnName == null) {
            indexColumn = -1;
            metaIndex = null;
        } else {
            indexColumn = getColumn(indexColumnName).getColumnId();
            IndexColumn[] indexCols = IndexColumn.wrap(new Column[] { columns[indexColumn] });
            metaIndex = new MetaIndex(this, indexCols, false);
        }
    }

    public void setObjectName(String name) {
        this.name = name;
    }

    public abstract String createColumns();

    public Column[] createColumns(String... names) {
        Column[] cols = new Column[names.length];
        for (int i = 0; i < names.length; i++) {
            String nameType = names[i];
            int idx = nameType.indexOf(' ');
            int dataType;
            String name;
            if (idx < 0) {
                dataType = database.getMode().lowerCaseIdentifiers ? Value.STRING_IGNORECASE
                        : Value.STRING;
                name = nameType;
            } else {
                dataType = DataType.getTypeByName(nameType.substring(idx + 1)).type;
                name = nameType.substring(0, idx);
            }
            cols[i] = new Column(name, dataType);
        }
        return cols;
    }

    /**
     * Generate the data for the given metadata table using the given first and
     * last row filters.
     *
     * @param session the session
     * @param first the first row to return
     * @param last the last row to return
     * @return the generated rows
     */
    public abstract ArrayList<Row> generateRows(ServerSession session, SearchRow first, SearchRow last);

    @Override
    public TableType getTableType() {
        return TableType.META_TABLE;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void removeChildrenAndResources(ServerSession session, DbObjectLock lock) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public Index getScanIndex(ServerSession session) {
        return new MetaIndex(this, IndexColumn.wrap(columns), true);
    }

    @Override
    public ArrayList<Index> getIndexes() {
        ArrayList<Index> list = new ArrayList<>(2);
        if (metaIndex == null) {
            return list;
        }
        list.add(getScanIndex(null));
        // TODO re-use the index
        list.add(metaIndex);
        return list;
    }

    @Override
    public long getMaxDataModificationId() {
        return database.getModificationDataId();
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public boolean canReference() {
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
        throw DbException.getInternalError();
    }

    @Override
    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    protected void add(ArrayList<Row> rows, String... strings) {
        Value[] values = new Value[strings.length];
        for (int i = 0; i < strings.length; i++) {
            String s = strings[i];
            Value v = (s == null) ? (Value) ValueNull.INSTANCE : ValueString.get(s);
            Column col = columns[i];
            v = col.convert(v);
            values[i] = v;
        }
        Row row = new Row(values);
        row.setKey(rows.size());
        rows.add(row);
    }
}
