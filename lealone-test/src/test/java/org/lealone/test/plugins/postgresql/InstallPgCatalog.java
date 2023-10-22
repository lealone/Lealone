/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.postgresql;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.ScriptReader;
import org.lealone.common.util.Utils;
import org.lealone.plugins.postgresql.server.PgServer;

public class InstallPgCatalog {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:lealone:embed/test";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();

        installPgCatalog(stmt);
        stmt.close();
        conn.close();
    }

    private static void installPgCatalog(Statement stat) throws SQLException {
        Reader r = null;
        try {
            r = Utils.getResourceAsReader(PgServer.PG_CATALOG_FILE);
            ScriptReader reader = new ScriptReader(r);
            while (true) {
                String sql = reader.readStatement();
                if (sql == null) {
                    break;
                }
                System.out.println(sql);
                stat.execute(sql);
            }
            reader.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Can not read pg_catalog resource");
        } finally {
            IOUtils.closeSilently(r);
        }
    }
}
