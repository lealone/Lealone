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
package org.lealone.test.orm;

import org.lealone.test.UnitTestBase;

public class TypeTest extends UnitTestBase {

    public static void main(String[] args) {
        TypeTest t = new TypeTest();
        t.setEmbedded(true);
        t.setInMemory(true);
        t.runTest();
    }

    @Override
    public void test() {
        // 21种类型，目前不支持GEOMETRY类型
        // INT
        // BOOLEAN
        // TINYINT
        // SMALLINT
        // BIGINT
        // IDENTITY
        // DECIMAL
        // DOUBLE
        // REAL
        // TIME
        // DATE
        // TIMESTAMP
        // BINARY
        // OTHER
        // VARCHAR
        // VARCHAR_IGNORECASE
        // CHAR
        // BLOB
        // CLOB
        // UUID
        // ARRAY
        execute("CREATE TABLE all_type (" //
                + " f1  INT," //
                + " f2  BOOLEAN," //
                + " f3  TINYINT," //
                + " f4  SMALLINT," //
                + " f5  BIGINT," //
                + " f6  IDENTITY," //
                + " f7  DECIMAL," //
                + " f8  DOUBLE," //
                + " f9  REAL," //
                + " f10 TIME," //
                + " f11 DATE," //
                + " f12 TIMESTAMP," //
                + " f13 BINARY," //
                + " f14 OTHER," //
                + " f15 VARCHAR," //
                + " f16 VARCHAR_IGNORECASE," //
                + " f17 CHAR," //
                + " f18 BLOB," //
                + " f19 CLOB," //
                + " f20 UUID," //
                + " f21 ARRAY" //
                + ")" //
                + " PACKAGE 'org.lealone.test.orm.generated'" //
                + " GENERATE CODE './src/test/java'");

        System.out.println("create table: all_type");
    }
}
