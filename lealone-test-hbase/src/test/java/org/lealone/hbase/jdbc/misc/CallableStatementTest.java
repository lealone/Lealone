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
package org.lealone.hbase.jdbc.misc;

import static junit.framework.Assert.assertEquals;

import java.sql.CallableStatement;
import java.sql.Types;

import org.junit.Test;
import org.lealone.hbase.jdbc.TestBase;

public class CallableStatementTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        test();
    }

    void init() throws Exception {
        stmt.executeUpdate("CREATE ALIAS IF NOT EXISTS MY_SQRT FOR \"java.lang.Math.sqrt\"");
    }

    void test() throws Exception {
        sql = "?= CALL MY_SQRT(?)";
        CallableStatement cs = conn.prepareCall(sql);
        cs.registerOutParameter(1, Types.DOUBLE); //sqlType其实被忽略了，所以设什么都没用
        cs.setDouble(2, 4.0);
        cs.execute();

        assertEquals(2.0, cs.getDouble(1));
        cs.close();
    }
}