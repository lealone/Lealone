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

import java.sql.PreparedStatement;

import org.junit.Test;
import org.lealone.test.sql.TestBase;

public class PreparedStatementTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        test();
    }

    void init() throws Exception {
        createTable("PreparedStatementTest");

        sql = "INSERT INTO PreparedStatementTest(pk, f1, f2) VALUES(?, ?, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, "01");
        ps.setString(2, "a1");
        ps.setInt(3, 10);
        ps.executeUpdate();

        ps.setString(1, "02");
        ps.setString(2, "a2");
        ps.setInt(3, 50);
        ps.executeUpdate();

        ps.setString(1, "03");
        ps.setString(2, "a3");
        ps.setInt(3, 30);
        ps.executeUpdate();

        ps.close();
    }

    void test() throws Exception {
        sql = "SELECT count(*) FROM PreparedStatementTest WHERE f2 >= ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, 30);
        rs = ps.executeQuery();
        rs.next();
        assertEquals(2, getIntValue(1, true));
        ps.close();
    }
}
