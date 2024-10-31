/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.Database;
import com.lealone.db.RunMode;
import com.lealone.db.index.Cursor;
import com.lealone.db.row.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.ValueInt;
import com.lealone.db.value.ValueString;
import com.lealone.storage.StorageSetting;

public class TableAlterHistory {

    private Table table;

    public void init(Connection conn, Database db) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS INFORMATION_SCHEMA.table_alter_history"
                    + " (id int, version int, alter_type int, columns varchar, PRIMARY KEY(id, version))"
                    + " PARAMETERS(" + StorageSetting.RUN_MODE.name() + "='"
                    + RunMode.CLIENT_SERVER.name() + "')");
            stmt.close();
            conn.commit();
            conn.close();
            table = db.findSchema(null, "INFORMATION_SCHEMA").findTableOrView(null,
                    "table_alter_history");
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public int getVersion(ServerSession session, int id) {
        Row row = table.getTemplateRow();
        row.setValue(0, ValueInt.get(id));
        Cursor cursor = table.getIndexes().get(1).find(session, row, row);
        int version = 0;
        while (cursor.next()) {
            row = cursor.get();
            int v = row.getValue(1).getInt();
            if (v > version)
                version = v;
        }
        return version;
    }

    public ArrayList<TableAlterHistoryRecord> getRecords(ServerSession session, int id, int versionMin,
            int versionMax) {
        Row min = table.getTemplateRow();
        min.setValue(0, ValueInt.get(id));
        min.setValue(1, ValueInt.get(versionMin));
        Row max = table.getTemplateRow();
        max.setValue(0, ValueInt.get(id));
        max.setValue(1, ValueInt.get(versionMax));
        Cursor cursor = table.getIndexes().get(1).find(session, min, max);
        ArrayList<TableAlterHistoryRecord> records = new ArrayList<>();
        while (cursor.next()) {
            Row row = cursor.get();
            records.add(new TableAlterHistoryRecord(row.getValue(0).getInt(), row.getValue(1).getInt(),
                    row.getValue(2).getInt(), row.getValue(3).getString()));
        }
        return records;
    }

    public void addRecord(ServerSession session, int id, int version, int alterType, String columns) {
        Row row = table.getTemplateRow();
        row.setValue(0, ValueInt.get(id));
        row.setValue(1, ValueInt.get(version));
        row.setValue(2, ValueInt.get(alterType));
        row.setValue(3, ValueString.get(columns));
        table.addRow(null, row);
    }

    public void deleteRecords(ServerSession session, int id) {
        Row row = table.getTemplateRow();
        row.setValue(0, ValueInt.get(id));
        Cursor cursor = table.getIndexes().get(1).find(session, row, row);
        if (cursor.next()) {
            row = cursor.get();
            table.removeRow(session, row);
        }
    }

    public static String getName() {
        return "table_alter_history";
    }
}
