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
package org.lealone.test.db.constraint;

import org.junit.Test;
import org.lealone.db.api.ErrorCode;

public class ConstraintUniqueTest extends ConstraintTestBase {

    private void init() {
        executeUpdate("DROP TABLE IF EXISTS mytable");
        executeUpdate("CREATE TABLE IF NOT EXISTS mytable (f1 int not null, f2 int not null, f3 int null)");
    }

    @Test
    public void run() {
        init();
        // PRIMARY KEY也是唯一约束
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c_unique PRIMARY KEY HASH(f1,f2)";
        executeUpdate(sql);
        assertFound("mytable", "c_unique");

        executeUpdate("insert into mytable(f1,f2) values(1, 9)");
        try {
            executeUpdate("insert into mytable(f1,f2) values(1, 9)");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.DUPLICATE_KEY_1);
        }

        sql = "ALTER TABLE mytable DROP CONSTRAINT IF EXISTS c_unique";
        executeUpdate(sql);
        assertNotFound("mytable", "c_unique");

        // 以下测试正常的唯一约束，字段没有null的情况
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c_unique2 UNIQUE KEY INDEX(f1) NOCHECK";
        executeUpdate(sql);
        assertFound("mytable", "c_unique2");

        executeUpdate("insert into mytable(f1,f2) values(2, 9)");
        try {
            executeUpdate("insert into mytable(f1,f2) values(2, 10)");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.DUPLICATE_KEY_1);
        }

        sql = "ALTER TABLE mytable DROP CONSTRAINT IF EXISTS c_unique2";
        executeUpdate(sql);
        assertNotFound("mytable", "c_unique2");

        init();
        // 以下测试正常的唯一约束，字段有null的情况
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c_unique3 UNIQUE KEY INDEX(f3) NOCHECK";
        executeUpdate(sql);
        assertFound("mytable", "c_unique3");

        executeUpdate("insert into mytable(f1,f2,f3) values(10, 20, 30)");
        executeUpdate("insert into mytable(f1,f2,f3) values(100, 200, null)");
        executeUpdate("insert into mytable(f1,f2,f3) values(1000, 2000, 3000)");
        try {
            executeUpdate("insert into mytable(f1,f2,f3) values(10000, 20000, 30)");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.DUPLICATE_KEY_1);
        }

        sql = "ALTER TABLE mytable DROP CONSTRAINT IF EXISTS c_unique3";
        executeUpdate(sql);
        assertNotFound("mytable", "c_unique3");
    }
}
