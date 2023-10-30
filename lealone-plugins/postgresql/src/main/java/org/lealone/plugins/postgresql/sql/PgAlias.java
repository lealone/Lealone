/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.lealone.db.Constants;
import org.lealone.plugins.postgresql.server.PgServer;
import org.lealone.plugins.postgresql.server.PgType;

/**
 * This class implements a subset of the PostgreSQL protocol as described here:
 * http://developer.postgresql.org/pgdocs/postgres/protocol.html
 * The PostgreSQL catalog is described here:
 * http://www.postgresql.org/docs/7.4/static/catalogs.html
 *
 * @author Thomas Mueller
 * @author Sergi Vladykin 2009-07-03 (convertType)
 * @author zhh
 */
// 以下方法在pg_catalog.sql中用到
public class PgAlias {

    /**
     * The Java implementation of the PostgreSQL function pg_get_indexdef. The
     * method is used to get CREATE INDEX command for an index, or the column
     * definition of one column in the index.
     *
     * @param conn the connection
     * @param indexId the index id
     * @param ordinalPosition the ordinal position (null if the SQL statement
     *            should be returned)
     * @param pretty this flag is ignored
     * @return the SQL statement or the column name
     */
    public static String getIndexColumn(Connection conn, int indexId, Integer ordinalPosition,
            Boolean pretty) throws SQLException {
        if (ordinalPosition == null || ordinalPosition.intValue() == 0) {
            PreparedStatement prep = conn
                    .prepareStatement("select sql from information_schema.indexes where id=?");
            prep.setInt(1, indexId);
            ResultSet rs = prep.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            return "";
        }
        PreparedStatement prep = conn.prepareStatement(
                "select column_name from information_schema.indexes where id=? and ordinal_position=?");
        prep.setInt(1, indexId);
        prep.setInt(2, ordinalPosition.intValue());
        ResultSet rs = prep.executeQuery();
        if (rs.next()) {
            return rs.getString(1);
        }
        return "";
    }

    /**
     * Get the name of the current schema.
     * This method is called by the database.
     *
     * @param conn the connection
     * @return the schema name
     */
    public static String getCurrentSchema(Connection conn) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("call schema()");
        rs.next();
        return rs.getString(1);
    }

    /**
     * Get the OID of an object.
     * This method is called by the database.
     *
     * @param conn the connection
     * @param tableName the table name
     * @return the oid
     */
    public static int getOid(Connection conn, String tableName) throws SQLException {
        if (tableName.startsWith("\"") && tableName.endsWith("\"")) {
            tableName = tableName.substring(1, tableName.length() - 1);
        }
        PreparedStatement prep = conn.prepareStatement("select oid from pg_class where relName = ?");
        prep.setString(1, tableName);
        ResultSet rs = prep.executeQuery();
        if (!rs.next()) {
            return 0;
        }
        return rs.getInt(1);
    }

    /**
     * Get the name of this encoding code.
     * This method is called by the database.
     *
     * @param code the encoding code
     * @return the encoding name
     */
    public static String getEncodingName(int code) {
        switch (code) {
        case 0:
            return "SQL_ASCII";
        case 6:
            return "UTF8";
        case 8:
            return "LATIN1";
        default:
            return code < 40 ? "UTF8" : "";
        }
    }

    public static int getEncodingCode(String name) {
        switch (name) {
        case "SQL_ASCII":
            return 0;
        case "UTF8":
            return 6;
        case "LATIN1":
            return 8;
        default:
            return 1;
        }
    }

    /**
     * Get the version. This method must return PostgreSQL to keep some clients
     * happy. This method is called by the database.
     *
     * @return the server name and version
     */
    public static String getVersion() {
        return "PostgreSQL " + PgServer.PG_VERSION + ", compiled by Lealone "
                + Constants.getFullVersion() + ", 64-bit";
    }

    public static String formatType(final int type, final int typmod) {
        switch (type) {
        case Types.BOOLEAN:
            return "PG_TYPE_BOOL";
        case Types.VARCHAR:
            return "PG_TYPE_VARCHAR";
        case Types.CLOB:
            return "PG_TYPE_TEXT";
        case Types.CHAR:
            return "PG_TYPE_BPCHAR";
        case Types.SMALLINT:
            return "PG_TYPE_INT2";
        case Types.INTEGER:
            return "PG_TYPE_INT4";
        case Types.BIGINT:
            return "PG_TYPE_INT8";
        case Types.DECIMAL:
            return "PG_TYPE_NUMERIC";
        case Types.REAL:
            return "PG_TYPE_FLOAT4";
        case Types.DOUBLE:
            return "PG_TYPE_FLOAT8";
        case Types.TIME:
            return "PG_TYPE_TIME";
        case Types.DATE:
            return "PG_TYPE_DATE";
        case Types.TIMESTAMP:
            return "PG_TYPE_TIMESTAMP_NO_TMZONE";
        case Types.VARBINARY:
            return "PG_TYPE_BYTEA";
        case Types.BLOB:
            return "PG_TYPE_OID";
        case Types.ARRAY:
            return "PG_TYPE_TEXTARRAY";
        default:
            return "PG_TYPE_UNKNOWN";
        }
    }

    /**
     * Get the current system time.
     * This method is called by the database.
     *
     * @return the current system time
     */
    public static Timestamp getStartTime() {
        return new Timestamp(System.currentTimeMillis());
    }

    /**
     * Get the user name for this id.
     * This method is called by the database.
     *
     * @param conn the connection
     * @param id the user id
     * @return the user name
     */
    public static String getUserById(Connection conn, int id) throws SQLException {
        PreparedStatement prep = conn
                .prepareStatement("SELECT NAME FROM INFORMATION_SCHEMA.USERS WHERE ID=?");
        prep.setInt(1, id);
        ResultSet rs = prep.executeQuery();
        if (rs.next()) {
            return rs.getString(1);
        }
        return null;
    }

    /**
     * Check if the this session has the given database privilege.
     * This method is called by the database.
     *
     * @param id the session id
     * @param privilege the privilege to check
     * @return true
     */
    public static boolean hasDatabasePrivilege(int id, String privilege) {
        return true;
    }

    /**
     * Check if the current session has access to this table.
     * This method is called by the database.
     *
     * @param table the table name
     * @param privilege the privilege to check
     * @return true
     */
    public static boolean hasTablePrivilege(String table, String privilege) {
        return true;
    }

    /**
     * Get the current transaction id.
     * This method is called by the database.
     *
     * @param table the table name
     * @param id the id
     * @return 1
     */
    public static int getCurrentTid(String table, String id) {
        return 1;
    }

    /**
     * Convert the SQL type to a PostgreSQL type
     *
     * @param type the SQL type
     * @return the PostgreSQL type
     */
    public static int convertType(int type) {
        return PgType.convertType(type);
    }
}
