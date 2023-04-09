/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;

public class TableAlterHistory {

    private PreparedStatement psGetVersion;
    private PreparedStatement psGetRecords;
    private PreparedStatement psAddRecord;
    private PreparedStatement psDeleteRecords;

    // 执行DROP DATABASE时调用这个方法，避免在删掉table_alter_history后还读它
    public void cleanPreparedStatements() {
        psGetVersion = null;
        psGetRecords = null;
        psAddRecord = null;
        psDeleteRecords = null;
    }

    public void init(Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS INFORMATION_SCHEMA.table_alter_history"
                    + " (id int, version int, alter_type int, columns varchar, PRIMARY KEY(id, version))");
            stmt.close();
            psGetVersion = conn.prepareStatement(
                    "select max(version) from INFORMATION_SCHEMA.table_alter_history where id = ?");
            psGetRecords = conn.prepareStatement(
                    "select id, version, alter_type, columns from INFORMATION_SCHEMA.table_alter_history "
                            + "where id = ? and version between ? and ?");
            psAddRecord = conn.prepareStatement(
                    "insert into INFORMATION_SCHEMA.table_alter_history values(?,?,?,?)");
            psDeleteRecords = conn
                    .prepareStatement("delete from INFORMATION_SCHEMA.table_alter_history where id = ?");
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public synchronized int getVersion(int id) {
        if (psGetVersion == null)
            DbException.throwInternalError();
        int version;
        try {
            psGetVersion.setInt(1, id);
            ResultSet rs = psGetVersion.executeQuery();
            if (rs.next()) {
                version = rs.getInt(1);
            } else {
                version = 0;
            }
            rs.close();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        return version;
    }

    public synchronized ArrayList<TableAlterHistoryRecord> getRecords(int id, int versionMin,
            int versionMax) {
        ArrayList<TableAlterHistoryRecord> records = new ArrayList<>();
        if (psGetRecords == null)
            return records;
        try {
            psGetRecords.setInt(1, id);
            psGetRecords.setInt(2, versionMin);
            psGetRecords.setInt(3, versionMax);
            ResultSet rs = psGetRecords.executeQuery();
            while (rs.next()) {
                records.add(new TableAlterHistoryRecord(rs.getInt(1), rs.getInt(2), rs.getInt(3),
                        rs.getString(4)));
            }
            rs.close();
            return records;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public synchronized void addRecord(int id, int version, int alterType, String columns) {
        if (psAddRecord == null)
            return;
        try {
            psAddRecord.setInt(1, id);
            psAddRecord.setInt(2, version);
            psAddRecord.setInt(3, alterType);
            psAddRecord.setString(4, columns);
            psAddRecord.executeUpdate();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public synchronized void deleteRecords(int id) {
        if (psDeleteRecords == null)
            return;
        try {
            psDeleteRecords.setInt(1, id);
            psDeleteRecords.executeUpdate();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public static String getName() {
        return "table_alter_history";
    }
}
