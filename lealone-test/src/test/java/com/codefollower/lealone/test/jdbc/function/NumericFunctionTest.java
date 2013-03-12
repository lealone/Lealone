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
package com.codefollower.lealone.test.jdbc.function;

import static junit.framework.Assert.assertEquals;
import org.junit.Test;

import com.codefollower.lealone.test.jdbc.TestBase;

public class NumericFunctionTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();

        sql = "SELECT abs(f3) FROM NumericFunctionTest WHERE _rowkey_ = '01'";
        assertEquals(1, getIntValue(1, true));
    }

    void init() throws Exception {
        createTable("NumericFunctionTest");

        //在分区1中保存1到11中的奇数
        stmt.executeUpdate("INSERT INTO NumericFunctionTest(_rowkey_, f1, cf1.f2, f3) VALUES('01', 'a1', 'b', -1)");
        stmt.executeUpdate("INSERT INTO NumericFunctionTest(_rowkey_, f1, cf1.f2, f3) VALUES('02', 'a1', 'b', 3)");
        stmt.executeUpdate("INSERT INTO NumericFunctionTest(_rowkey_, f1, cf1.f2, f3) VALUES('03', 'a1', 'b', 5)");
        stmt.executeUpdate("INSERT INTO NumericFunctionTest(_rowkey_, f1, cf1.f2, f3) VALUES('04', 'a2', 'b', 7)");
        stmt.executeUpdate("INSERT INTO NumericFunctionTest(_rowkey_, f1, cf1.f2, f3) VALUES('05', 'a2', 'b', 9)");
        stmt.executeUpdate("INSERT INTO NumericFunctionTest(_rowkey_, f1, cf1.f2, f3) VALUES('06', 'a2', 'b', 11)");
    }

}
