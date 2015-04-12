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
package org.lealone.test.sql.misc;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;
import org.lealone.test.TestBase;

public class SubqueryTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        testSelect();
    }

    void init() throws Exception {
        createTable("SubqueryTest");

        executeUpdate("INSERT INTO SubqueryTest(pk, f1, f2) VALUES('01', 'a1', 10)");
        executeUpdate("INSERT INTO SubqueryTest(pk, f1, f2) VALUES('02', 'a2', 50)");
        executeUpdate("INSERT INTO SubqueryTest(pk, f1, f2) VALUES('03', 'a3', 30)");

        executeUpdate("INSERT INTO SubqueryTest(pk, f1, f2) VALUES('04', 'a4', 40)");
        executeUpdate("INSERT INTO SubqueryTest(pk, f1, f2) VALUES('05', 'a5', 20)");
        executeUpdate("INSERT INTO SubqueryTest(pk, f1, f2) VALUES('06', 'a6', 60)");
    }

    void testSelect() throws Exception {
        //scalar subquery
        sql = "SELECT count(*) FROM SubqueryTest WHERE pk>='01'" //
                + " AND f2 >= (SELECT f2 FROM SubqueryTest WHERE pk='01')";
        assertEquals(6, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE pk>='01'" //
                + " AND EXISTS(SELECT f2 FROM SubqueryTest WHERE pk='01' AND f1='a1')";
        assertEquals(6, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE pk>='01'" //
                + " AND f2 IN(SELECT f2 FROM SubqueryTest WHERE pk>='04')";
        assertEquals(3, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE pk>='01'" //
                + " AND f2 < ALL(SELECT f2 FROM SubqueryTest WHERE pk>='04')";
        assertEquals(1, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE pk>='01'" //
                + " AND f2 < ANY(SELECT f2 FROM SubqueryTest WHERE pk>='04')";
        assertEquals(5, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE pk>='01'" //
                + " AND f2 < SOME(SELECT f2 FROM SubqueryTest WHERE pk>='04')";
        assertEquals(5, getIntValue(1, true));
    }
}
