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
package org.lealone.test.sql.function;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class StringFunctionTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();

        sql = "SELECT LENGTH(f1), UPPER(f1), LOWER(f1) FROM StringFunctionTest WHERE pk = '01'";
        assertEquals(2, getIntValue(1, false));
        assertEquals("A1", getStringValue(2, false));
        assertEquals("a1", getStringValue(3, true));
    }

    void init() throws Exception {
        createTable("StringFunctionTest");

        // 在分区1中保存1到11中的奇数
        executeUpdate("INSERT INTO StringFunctionTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', -1)");
        executeUpdate("INSERT INTO StringFunctionTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 3)");
        executeUpdate("INSERT INTO StringFunctionTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 5)");
        executeUpdate("INSERT INTO StringFunctionTest(pk, f1, f2, f3) VALUES('04', 'a2', 'b', 7)");
        executeUpdate("INSERT INTO StringFunctionTest(pk, f1, f2, f3) VALUES('05', 'a2', 'b', 9)");
        executeUpdate("INSERT INTO StringFunctionTest(pk, f1, f2, f3) VALUES('06', 'a2', 'b', 11)");
    }
}
