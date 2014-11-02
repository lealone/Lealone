/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.lealone.test.benchmark;

public class BenchCreateTable extends BenchBase {
    public static void main(String[] args) throws Exception {
        new BenchCreateTable().run();
    }

    public void run() throws Exception {
        init();

        loop = 1;
        //这个for用于热身
        for (int i = 0; i < loop; i++) {
            total += testHBaseCreateTable(i);
        }
        avg();

        loop = 3;

        for (int i = 0; i < loop; i++) {
            total += testHBaseCreateTable(i);
        }
        avg();

        for (int i = 0; i < loop; i++) {
            total += testSQLDynamicCreateTable(i);
        }
        avg();

        for (int i = 0; i < loop; i++) {
            total += testSQLStaticCreateTable(i);
        }
        avg();
    }

    long testHBaseCreateTable(int tableNameId) throws Exception {
        long start = System.nanoTime();

        tableName = "testHBaseCreateTable" + tableNameId;
        deleteTable();
        createTable("cf");

        long end = System.nanoTime();
        p("testHBase()", end - start);
        return end - start;
    }

    long testSQLDynamicCreateTable(int tableNameId) throws Exception {
        long start = System.nanoTime();

        tableName = "testSQLDynamicCreateTable" + tableNameId;
        stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS " + tableName + " (" //
                + "COLUMN FAMILY cf(id int, name varchar(500), age long, salary double))");

        long end = System.nanoTime();
        p("testSQLDynamicCreateTable()", end - start);
        return end - start;
    }

    long testSQLStaticCreateTable(int tableNameId) throws Exception {
        long start = System.nanoTime();

        tableName = "testSQLStaticCreateTable" + tableNameId;
        stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName //
                + "(id int, name varchar(500), age long, salary float)");

        long end = System.nanoTime();
        p("testSQLStaticCreateTable()", end - start);
        return end - start;
    }
}
