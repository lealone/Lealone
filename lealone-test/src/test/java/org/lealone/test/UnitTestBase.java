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
package org.lealone.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

//一个标记类，标识它的子类是进行单元测试的
public class UnitTestBase extends TestBase {

    public UnitTestBase() {
        initTransactionEngine();
    }

    public void execute(String sql) {
        try (Connection conn = DriverManager.getConnection(getURL()); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void runTest() {
        runTest(true, true);
    }

    public void runTest(boolean isEmbeddedMemoryMode, boolean closeTransactionEngine) {
        if (isEmbeddedMemoryMode) {
            setEmbedded(true);
            setInMemory(true);
        }

        try {
            test();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (closeTransactionEngine)
                closeTransactionEngine();
        }
    }

    protected void test() throws Exception {
        // do nothing
    }
}
