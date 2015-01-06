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
package org.lealone.transaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.lealone.engine.Constants;
import org.lealone.engine.SystemDatabase;
import org.lealone.message.DbException;
import org.lealone.util.JdbcUtils;

public class TimestampServiceTable {

    private TimestampServiceTable() {
    }

    private static final long TIMESTAMP_BATCH = Long.valueOf(System.getProperty(Constants.PROJECT_NAME_PREFIX
            + "transaction.timestamp.batch", "100000"));

    private static PreparedStatement updateLastMaxTimestamp;
    private static PreparedStatement getLastMaxTimestamp;

    private static long first;
    private static long last;
    private static long maxTimestamp;

    public static synchronized void init() {
        if (getLastMaxTimestamp != null)
            return;

        createTableIfNotExists();

        first = last = maxTimestamp = getLastMaxTimestamp();
        addBatch();
    }

    private static void createTableIfNotExists() {
        ResultSet rs = null;
        Statement stmt = null;
        Connection conn = SystemDatabase.getConnection();
        try {
            stmt = conn.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS timestamp_service_table" //
                    + "(last_max_timestamp BIGINT PRIMARY KEY)");

            updateLastMaxTimestamp = conn.prepareStatement("UPDATE timestamp_service_table SET last_max_timestamp = ?");
            getLastMaxTimestamp = conn.prepareStatement("SELECT last_max_timestamp FROM timestamp_service_table");

            rs = getLastMaxTimestamp.executeQuery();
            if (!rs.next()) {
                stmt.executeUpdate("INSERT INTO timestamp_service_table VALUES(0)");
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stmt);
        }
    }

    private static void updateLastMaxTimestamp(long lastMaxTimestamp) {
        try {
            updateLastMaxTimestamp.setLong(1, lastMaxTimestamp);
            updateLastMaxTimestamp.executeUpdate();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private static long getLastMaxTimestamp() {
        long lastMaxTimestamp = 0;
        ResultSet rs = null;
        try {
            rs = getLastMaxTimestamp.executeQuery();
            if (rs.next()) {
                lastMaxTimestamp = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        } finally {
            JdbcUtils.closeSilently(rs);
        }

        return lastMaxTimestamp;
    }

    private static void addBatch() {
        maxTimestamp += TIMESTAMP_BATCH;
        updateLastMaxTimestamp(maxTimestamp);
    }

    public synchronized static void reset() {
        first = last = maxTimestamp = 0;
        updateLastMaxTimestamp(0);
        addBatch();
    }

    //事务用奇数版本号
    public synchronized static long nextOdd() {
        if (last >= maxTimestamp)
            addBatch();

        long delta;
        if (last % 2 == 0)
            delta = 1;
        else
            delta = 2;

        last += delta;
        return last;
    }

    //非事务用偶数版本号
    public synchronized static long nextEven() {
        if (last >= maxTimestamp)
            addBatch();

        long delta;
        if (last % 2 == 0)
            delta = 2;
        else
            delta = 1;
        last += delta;
        return last;
    }

    public static long first() {
        return first;
    }

    public static String toS() {
        return "TimestampService(first: " + first + ", last: " + last + ", max: " + maxTimestamp + ")";
    }
}
