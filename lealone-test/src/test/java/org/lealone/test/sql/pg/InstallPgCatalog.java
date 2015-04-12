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
package org.lealone.test.sql.pg;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.lealone.message.DbException;
import org.lealone.test.sql.TestBase;
import org.lealone.util.IOUtils;
import org.lealone.util.ScriptReader;
import org.lealone.util.Utils;

public class InstallPgCatalog {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:lealone:tcp://localhost:5210/" + TestBase.db;
        Connection conn = DriverManager.getConnection(url, "sa", "");
        Statement stmt = conn.createStatement();

        installPgCatalog(stmt);
        stmt.close();
        conn.close();
    }

    private static void installPgCatalog(Statement stat) throws SQLException {
        Reader r = null;
        try {
            r = new InputStreamReader(new ByteArrayInputStream(Utils.getResource("/org/lealone/res/pg_catalog.sql")));
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
