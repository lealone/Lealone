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
package org.lealone.test.sql.ddl;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.lealone.test.TestBase;

public class CreateSequenceTest extends TestBase {
    @Test
    public void testSequence() throws Exception {
        try {
            //加了BELONGS_TO_TABLE就删不掉了
            stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS myseq1 START WITH 1000 INCREMENT BY 1 CACHE 20 BELONGS_TO_TABLE");
            stmt.executeUpdate("DROP SEQUENCE IF EXISTS myseq1");
            Assert.fail("not throw SQLException");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("belongs to a table"));
        }
        stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS myseq2 START WITH 1000 INCREMENT BY 1 CACHE 20");
        sql = "select myseq2.CURRVAL, myseq2.NEXTVAL";

        assertEquals(999, getLongValue(1)); //因为先CURRVAL后NEXTVAL，所以CURRVAL是value - increment = 1000 - 1 = 999
        assertEquals(1000, getLongValue(2, true));

        //再执行一次前面的sql
        assertEquals(1000, getLongValue(1));
        assertEquals(1001, getLongValue(2, true));

        //"IF EXISTS"可放在SEQUENCE名称的前后
        stmt.executeUpdate("DROP SEQUENCE IF EXISTS myseq2");
        stmt.executeUpdate("DROP SEQUENCE myseq2 IF EXISTS");
    }

    @Test
    public void run() throws Exception {
        init();
        testInsert();
        testSelect();
    }

    void init() throws Exception {
        createTable("CreateSequenceTest");
        stmt.executeUpdate("DROP SEQUENCE IF EXISTS myseq3");
        stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS myseq3 START WITH 1000 INCREMENT BY 1 CACHE 3");
    }

    void testInsert() throws Exception {
        //f1没有加列族前缀，默认是cf1，按CREATE HBASE TABLE中的定义顺序，哪个在先默认就是哪个
        //或者在表OPTIONS中指定DEFAULT_COLUMN_FAMILY_NAME参数
        stmt.executeUpdate("INSERT INTO CreateSequenceTest(pk, f1) VALUES('01', myseq3.NEXTVAL)");
        stmt.executeUpdate("INSERT INTO CreateSequenceTest(pk, f1) VALUES('26', myseq3.NEXTVAL)");
        stmt.executeUpdate("INSERT INTO CreateSequenceTest(pk, f1) VALUES('51', myseq3.NEXTVAL)");
        stmt.executeUpdate("INSERT INTO CreateSequenceTest(pk, f1) VALUES('76', myseq3.NEXTVAL)");

        sql = "SELECT pk, f1 FROM CreateSequenceTest";
        printResultSet();
    }

    void testSelect() throws Exception {
        sql = "SELECT pk, f1 FROM CreateSequenceTest";
        printResultSet();
    }
}
