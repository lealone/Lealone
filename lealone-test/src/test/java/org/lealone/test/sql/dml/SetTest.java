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
package org.lealone.test.sql.dml;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class SetTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        executeUpdate("SET @v1 1");
        executeUpdate("SET @v2 TO 2");
        executeUpdate("SET @v3 = 3");

        sql = "select @v1, @v2, @v3";
        assertEquals(1, getIntValue(1));
        assertEquals(2, getIntValue(2));
        assertEquals(3, getIntValue(3, true));
    }
}