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
package org.yourbase.jdbc.function;

import org.junit.Test;
import org.yourbase.jdbc.TestBase;

public class SystemFunctionTest extends TestBase {
    @Test
    public void run() throws Exception {
        test();
    }

    void test() throws Exception {
        sql = "SELECT DECODE(RAND()>0.5, 0, 'Red', 1, 'Black')";

        sql = "SELECT DECODE(RAND()>0.5, 0, 'Red1', 0, 'Red2', 1, 'Black1', 1, 'Black2')";

        sql = "SELECT DECODE(RAND()>0.5, 2, 'Red1', 2, 'Red2', 2, 'Black1', 2)";
        sql = "SELECT DECODE(RAND()>0.5, 2, 'Red1', 2, 'Red2', 2, 'Black1', 2, 'Black2')";

        sql = "SELECT DECODE(0, 0, 'v1', 0,/'v2', 1, 'v3', 1, 'v4')";

        //ROW_NUMBER函数虽然定义了，但ROW_NUMBER()函数无效，不支持这样的语法
        sql = "SELECT ROW_NUMBER()";
        //ROWNUM函数虽然没有定义，但ROWNUM()是有效，Parser在解析时把他当成ROWNUM伪字段处理
        //当成了org.h2.expression.Rownum，见org.h2.command.Parser.readTerm()
        sql = "SELECT ROWNUM()";
        //这样就没问题了,在这个方法中org.h2.command.Parser.readFunction(Schema, String)
        //把ROW_NUMBER转成org.h2.expression.Rownum了
        sql = "SELECT ROW_NUMBER()OVER()";

        //相等返回null，不相等返回v0
        sql = "SELECT NULLIF(1,2)"; //1
        sql = "SELECT NULLIF(1,1)"; //null

        sql = "SELECT DATABASE()";
        sql = "SELECT USER(), CURRENT_USER()";
        sql = "SELECT IDENTITY(), SCOPE_IDENTITY()";
        sql = "SELECT LOCK_TIMEOUT()";
        sql = "SELECT MEMORY_FREE(), MEMORY_USED()";

        sql = "SELECT GREATEST(1,2,3), LEAST(1,2,3)";

        sql = "SELECT ARRAY_GET(('Hello', 'World'), 2), ARRAY_LENGTH(('Hello', 'World')), "
                + "ARRAY_CONTAINS(('Hello', 'World'), 'Hello')";

        //sql = "SELECT CASE(1>0, 1, b<0, 2)"; //不能这样用

        sql = "SELECT SET(@v, 1), CASE @v WHEN 0 THEN 'No' WHEN 1 THEN 'One' ELSE 'Some' END";
        sql = "SELECT SET(@v, 11), CASE WHEN @v<10 THEN 'Low' ELSE 'High' END";
        stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS SEQ_ID");

        sql = "SELECT CURRVAL('SEQ_ID'), NEXTVAL('SEQ_ID')";
        printResultSet();
    }

}
