/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.table.TableAlterHistoryRecord;

public class DbObjectVersionManager {

    private PreparedStatement psGetVersion;
    private PreparedStatement psUpdateVersion;

    private PreparedStatement psAddTableAlterHistoryRecord;
    private PreparedStatement psGetTableAlterHistoryRecord;
    private PreparedStatement psDeleteTableAlterHistoryRecord;

    // 执行DROP DATABASE时调用这个方法，避免在删掉table_alter_history后还读它
    public void cleanPreparedStatements() {
        psGetVersion = null;
        psUpdateVersion = null;
        psAddTableAlterHistoryRecord = null;
        psGetTableAlterHistoryRecord = null;
        psDeleteTableAlterHistoryRecord = null;
    }

    public void initDbObjectVersionTable(Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS db_object_version (id int PRIMARY KEY, version int)");
            stmt.execute("CREATE TABLE IF NOT EXISTS table_alter_history"
                    + " (id int, version int, alter_type int, columns varchar, PRIMARY KEY(id, version))");
            stmt.close();
            psGetVersion = conn.prepareStatement("select version from db_object_version where id = ?");
            psUpdateVersion = conn
                    .prepareStatement("update db_object_version set version = ? where id = ?");

            psAddTableAlterHistoryRecord = conn
                    .prepareStatement("insert into table_alter_history values(?,?,?,?)");
            psGetTableAlterHistoryRecord = conn
                    .prepareStatement("select id, version, alter_type, columns from table_alter_history "
                            + "where id = ? and version between ? and ?");
            psDeleteTableAlterHistoryRecord = conn
                    .prepareStatement("delete from table_alter_history where id = ?");
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public synchronized void addTableAlterHistoryRecord(int id, int version, int alterType,
            String columns) {
        if (psAddTableAlterHistoryRecord == null)
            return;
        try {
            psAddTableAlterHistoryRecord.setInt(1, id);
            psAddTableAlterHistoryRecord.setInt(2, version);
            psAddTableAlterHistoryRecord.setInt(3, alterType);
            psAddTableAlterHistoryRecord.setString(4, columns);
            psAddTableAlterHistoryRecord.executeUpdate();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public synchronized void deleteTableAlterHistoryRecord(int id) {
        if (psDeleteTableAlterHistoryRecord == null)
            return;
        try {
            psDeleteTableAlterHistoryRecord.setInt(1, id);
            psDeleteTableAlterHistoryRecord.executeUpdate();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public synchronized ArrayList<TableAlterHistoryRecord> getTableAlterHistoryRecord(int id,
            int versionMin, int versionMax) {
        ArrayList<TableAlterHistoryRecord> records = new ArrayList<>();
        if (psGetTableAlterHistoryRecord == null)
            return records;
        try {
            psGetTableAlterHistoryRecord.setInt(1, id);
            psGetTableAlterHistoryRecord.setInt(2, versionMin);
            psGetTableAlterHistoryRecord.setInt(3, versionMax);
            ResultSet rs = psGetTableAlterHistoryRecord.executeQuery();
            while (rs.next()) {
                records.add(new TableAlterHistoryRecord(rs.getInt(1), rs.getInt(2), rs.getInt(3),
                        rs.getString(4)));
            }
            return records;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public synchronized void updateVersion(int id, int version) {
        if (psUpdateVersion == null)
            return;
        try {
            psUpdateVersion.setInt(1, version);
            psUpdateVersion.setInt(2, id);
            psUpdateVersion.executeUpdate();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public synchronized int getVersion(int id) {
        if (psGetVersion == null)
            return -1;
        int version;
        try {
            psGetVersion.setInt(1, id);
            ResultSet rs = psGetVersion.executeQuery();
            if (rs.next()) {
                version = rs.getInt(1);
            } else {
                version = 1;
                updateVersion(id, version);
            }
            rs.close();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        return version;
    }

    public static boolean isDbObjectVersionTable(String tableName) {
        return "db_object_version".equalsIgnoreCase(tableName)
                || "table_alter_history".equalsIgnoreCase(tableName);
    }
}
