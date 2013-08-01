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
package com.codefollower.lealone.test.jdbc.misc;

import static junit.framework.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

import com.codefollower.lealone.test.jdbc.TestBase;

public class ShortColumnNameTest extends TestBase {
    private String db = com.codefollower.lealone.hbase.util.HBaseUtils.HBASE_DB_NAME;

    @Test
    public void run() throws Exception {
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS ShortColumnNameTest (" //
                + "COLUMN FAMILY cf1," //
                + "COLUMN FAMILY cf2(f2 int)," //
                + "COLUMN FAMILY cf3(f3 int)" //
                + ")");

        testInsert();
        testUpdate();
        testSelect();
    }

    private void testInsert() throws Exception {
        //f1是一个动态字段，因为没有指定列族名，而cf1在建表时排在第一，所以它是默认列族名，所以f1将归入cf1
        //f2在cf2中定义，虽然没有指定列族名，但是也能识别出它属于cf2
        //f3在cf3中定义，虽然没有指定列族名，但是也能识别出它属于cf3
        stmt.executeUpdate("INSERT INTO ShortColumnNameTest(_rowkey_, f1, f2, f3) VALUES('01', 1, 2, 3)");

        //f4前面加了cf2前缀，虽然cf2未定义f4，但是此时f4归入cf2
        stmt.executeUpdate("INSERT INTO ShortColumnNameTest(_rowkey_, f1, cf2.f4, f3) VALUES('01', 1, 4, 3)");

        //cf1.f4前面加了cf1前缀，虽然cf1未定义f4，但是此时f4归入cf1
        stmt.executeUpdate("INSERT INTO ShortColumnNameTest(_rowkey_, cf1.f4, cf2.f4, f3) VALUES('01', 4, 44, 3)");

        //因为cf1和cf2此时都有f4，而cf1在建表时排在第一，所以它是默认列族名，此时1个f4未加列族前缀，所以都属于cf1
        stmt.executeUpdate("INSERT INTO ShortColumnNameTest(_rowkey_, f4, cf2.f4, f3) VALUES('01', 4, 444, 3)");

        sql = "SELECT f1, f2, f3, cf1.f4, cf2.f4 FROM ShortColumnNameTest";
        assertEquals(1, getIntValue(1));
        assertEquals(2, getIntValue(2));
        assertEquals(3, getIntValue(3));
        assertEquals(4, getIntValue(4));
        assertEquals(444, getIntValue(5, true));
    }

    private void testUpdate() throws Exception {
        stmt.executeUpdate("UPDATE ShortColumnNameTest t SET " + db + ".public.t.cf1.f1 = 10 WHERE _rowkey_='01'");
        stmt.executeUpdate("UPDATE ShortColumnNameTest t SET public.t.cf1.f1 = 10 WHERE _rowkey_='01'");
        stmt.executeUpdate("UPDATE ShortColumnNameTest t SET t.cf1.f1 = 10 WHERE _rowkey_='01'");
        stmt.executeUpdate("UPDATE ShortColumnNameTest t SET cf1.f1 = 10 WHERE _rowkey_='01'");
        stmt.executeUpdate("UPDATE ShortColumnNameTest t SET f1 = 10 WHERE _rowkey_='01'");

        stmt.executeUpdate("UPDATE ShortColumnNameTest t SET t.f1 = 100 WHERE _rowkey_='01'");
        stmt.executeUpdate("UPDATE ShortColumnNameTest t SET t.f3 = 30 WHERE _rowkey_='01'");

        //t.f4没有指定列族，因为cf1和cf2都有f4，而cf1在建表时排在第一，所以它是默认列族名，所以f4属于cf1
        stmt.executeUpdate("UPDATE ShortColumnNameTest t SET t.f4 = 40 WHERE _rowkey_='01'");

        sql = "SELECT f1, f2, f3, cf1.f4, cf2.f4 FROM ShortColumnNameTest";
        assertEquals(100, getIntValue(1));
        assertEquals(2, getIntValue(2));
        assertEquals(30, getIntValue(3));
        assertEquals(40, getIntValue(4));
        assertEquals(444, getIntValue(5, true));

        try {
            //t2是一个错误的别名
            sql = "UPDATE ShortColumnNameTest t SET t2.f4 = 40 WHERE _rowkey_='01'";
            stmt.executeUpdate(sql);
            Assert.fail(sql);
        } catch (SQLException e) {
            //e.printStackTrace();
        }

        try {
            //cf3中没有f4
            sql = "UPDATE ShortColumnNameTest t SET cf3.f4 = 40 WHERE _rowkey_='01'";
            stmt.executeUpdate(sql);
            Assert.fail(sql);
        } catch (SQLException e) {
            //e.printStackTrace();
        }
    }

    private void testSelect() throws Exception {
        sql = "SELECT  " + db + ".public.t.cf1.f1 FROM ShortColumnNameTest t WHERE _rowkey_='01'";
        printResultSet();

        sql = "SELECT public.t.cf1.f1 FROM ShortColumnNameTest t WHERE _rowkey_='01'";
        printResultSet();

        sql = "SELECT t.cf1.f1 FROM ShortColumnNameTest t WHERE _rowkey_='01'";
        printResultSet();

        sql = "SELECT cf1.f1 FROM ShortColumnNameTest t WHERE _rowkey_='01'";
        printResultSet();

        sql = "SELECT f1 FROM ShortColumnNameTest t WHERE _rowkey_='01'";
        printResultSet();

        sql = "SELECT t.f1 FROM ShortColumnNameTest t WHERE _rowkey_='01'";
        printResultSet();

        sql = "SELECT t.f3 FROM ShortColumnNameTest t WHERE _rowkey_='01'";
        printResultSet();
    }
}
