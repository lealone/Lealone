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
package org.lealone.test.client;

import java.sql.ResultSet;

import org.junit.Test;
import org.lealone.common.trace.TraceSystem;
import org.lealone.test.sql.SqlTestBase;

public class TraceTest extends SqlTestBase {

    public TraceTest() {
        super("TraceTestDB");
        enableTrace(TraceSystem.DEBUG);
    }

    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS TraceTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS TraceTest (f1 int, f2 long)");
        stmt.executeUpdate("INSERT INTO TraceTest(f1, f2) VALUES(1, 1)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM TraceTest");
        rs.close();
    }
}