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
package org.lealone.test.sql.ddl;

import org.junit.Test;
import org.lealone.test.TestBase;

public class CreateTableTest extends TestBase {
    @Test
    public void run() throws Exception {
        //stmt.executeUpdate("DROP TABLE IF EXISTS CreateTableTest");

        //如果只有一个列族，可以用最简化的方式定义表
        sql = "CREATE TABLE IF NOT EXISTS CreateTableTest(" //
                + "TABLE OPTIONS(DEFERRED_LOG_FLUSH='false')," //可选的
                + "SPLIT KEYS(10, 40, 80)," //可选的
                + "COLUMN FAMILY OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true)," //可选的

                + "f1 int primary key, f2 long" //
                + ")";
        stmt.executeUpdate(sql);

        //定义多个列族
        sql = "CREATE TABLE IF NOT EXISTS CreateTableTest(" //
                + "TABLE OPTIONS(DEFERRED_LOG_FLUSH='false')," //
                + "SPLIT KEYS(10, 40, 80)," //

                + "COLUMN FAMILY cf1 (" //
                + "    OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true), " //
                + "    f1 int, " //
                + "    f2 varchar, " //
                + "    f3 date" //
                + ")," //

                + "COLUMN FAMILY cf2 (" //
                + "    OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true)" //
                + ")," //

                + "COLUMN FAMILY cf3" //

                + ")";
        stmt.executeUpdate(sql);

        sql = "CREATE TABLE IF NOT EXISTS CreateTableTest(" //
                + "OPTIONS(DEFERRED_LOG_FLUSH='false')," //
                + "TABLE OPTIONS(DEFERRED_LOG_FLUSH='false')," //
                + "SPLIT KEYS(10, 40, 80)," //
                + "COLUMN FAMILY OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true)," //

                + "f1 int primary key, f2 long, " //

                //f1, f2重复了
                //                + "COLUMN FAMILY cf1 (" //
                //                + "    OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true), " //
                //                + "    f1 int, " //
                //                + "    f2 varchar, " //
                //                + "    f3 date" //
                //                + ")," //

                //cf1是默认列族，所以f1, f2归到cf1
                + "COLUMN FAMILY cf1 (" //
                + "    OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true), " //
                + "    f3 int, " //
                + "    f4 varchar, " //
                + "    f5 date" //
                + ")," //

                + "COLUMN FAMILY cf2 (" //
                + "    OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true)" //
                + ")," //

                + "COLUMN FAMILY cf3" //

                + ")";
        stmt.executeUpdate(sql);

    }
}
