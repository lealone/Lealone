/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.dbobject.index;

import java.util.ArrayList;

import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.TableBase;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.util.New;
import org.lealone.util.ValueHashMap;
import org.lealone.value.Value;

/**
 * A non-unique index based on an in-memory hash map.
 *
 * @author Sergi Vladykin
 */
public class NonUniqueHashIndex extends HashIndex {

    private ValueHashMap<ArrayList<Long>> rows;
    private final TableBase tableData;
    private long rowCount;

    public NonUniqueHashIndex(TableBase table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        super(table, id, indexName, columns, indexType);
        this.tableData = table;
        reset();
    }

    private void reset() {
        rows = ValueHashMap.newInstance();
        rowCount = 0;
    }

    public void truncate(Session session) {
        reset();
    }

    public void add(Session session, Row row) {
        Value key = row.getValue(indexColumn);
        ArrayList<Long> positions = rows.get(key);
        if (positions == null) {
            positions = New.arrayList();
            rows.put(key, positions);
        }
        positions.add(row.getKey());
        rowCount++;
    }

    public void remove(Session session, Row row) {
        if (rowCount == 1) {
            // last row in table
            reset();
        } else {
            Value key = row.getValue(indexColumn);
            ArrayList<Long> positions = rows.get(key);
            if (positions.size() == 1) {
                // last row with such key
                rows.remove(key);
            } else {
                positions.remove(row.getKey());
            }
            rowCount--;
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (first == null || last == null) {
            throw DbException.throwInternalError();
        }
        if (first != last) {
            if (compareKeys(first, last) != 0) {
                throw DbException.throwInternalError();
            }
        }
        ArrayList<Long> positions = rows.get(first.getValue(indexColumn));
        return new NonUniqueHashCursor(session, tableData, positions);
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

}
