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
package org.lealone.test.sql.function;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

//总共26个日期与时间函数
public class DateAndTimeFunctionTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        test0();
    }

    void init() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS DateAndTimeFunctionTest");
        executeUpdate("CREATE TABLE DateAndTimeFunctionTest (pk int NOT NULL PRIMARY KEY, " + //
                "d date, t time, ts timestamp)");

        insert();
    }

    void insert() throws Exception {
        stmt.executeUpdate("INSERT INTO DateAndTimeFunctionTest(pk, d, t, ts) "
                + "VALUES(1, DATE '2012-12-21', TIME '11:59:59.999', TIMESTAMP '2012-12-21 11:59:59')");
    }

    void test0() throws Exception {
        sql = "SELECT " //
                // 以下这21个日期与时间函数在这个方法中:
                // org.lealone.expression.Function.getSimpleValue(Session, Value, Expression[], Value[])
                // ----------------------------------------------------------------------------------------
                + "DAYNAME(d), " // 星期几，英文形式，如星期5是: Friday
                + "DAY_OF_MONTH(d), " // 多少号
                + "DAY_OF_WEEK(d), " // 一周的第几天，第一天Sunday是
                + "DAY_OF_YEAR(d), " // 一年的第几天
                + "HOUR(ts), " //
                + "MINUTE(ts), " //
                + "MONTHNAME(d), " //
                + "QUARTER(d), " //
                + "SECOND(ts), " //
                + "WEEK(d), " //
                + "YEAR(d), " //

                + "ISO_YEAR(d), " //
                + "ISO_WEEK(d), " //
                + "ISO_DAY_OF_WEEK(d), " // 一周的第几天，第一天Monday是

                + "CURDATE(), CURRENT_DATE(), CURRENT_DATE, " //
                + "CURTIME(), CURRENT_TIME(), CURRENT_TIME, " //
                + "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP, NOW(), NOW(0), NOW(1), NOW(2), NOW(3), NOW(4), " // now后的数字是刻度

                // 以下这5个日期与时间函数在这个方法中:
                // org.lealone.expression.Function.getValueWithArgs(Session, Expression[])
                // ----------------------------------------------------------------------------------------
                + "DATEADD('MONTH', 1, d), " //
                + "DATEDIFF('YEAR', DATE '2001-01-31', DATE '1999-01-31'), " //
                + "EXTRACT(DAY FROM d), " //
                + "EXTRACT(YEAR FROM d), " //

                + "FORMATDATETIME(ts, 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT'), " //
                + "FORMATDATETIME(null, 'EEE, d MMM yyyy HH:mm:ss z'), " //
                + "FORMATDATETIME(ts, null), " //

                + "PARSEDATETIME('Sat, 3 Feb 2001 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT') " //

                + "FROM DateAndTimeFunctionTest";

        // 世界末日那天是星期五
        assertEquals("friday", getStringValue(1).toLowerCase());
        assertEquals(21, getIntValue(2));
        assertEquals(6, getIntValue(3));
        assertEquals(356, getIntValue(4));
        assertEquals(11, getIntValue(5));
        assertEquals(59, getIntValue(6));
        assertEquals("december", getStringValue(7).toLowerCase());
        assertEquals(4, getIntValue(8));
        assertEquals(59, getIntValue(9));
        assertEquals(51, getIntValue(10));
        assertEquals(2012, getIntValue(11));
        assertEquals(2012, getIntValue(12));
        assertEquals(51, getIntValue(13));
        assertEquals(5, getIntValue(14));

        assertTrue(getStringValue(15).equals(getStringValue(16)) && getStringValue(15).equals(getStringValue(17)));
        assertTrue(getStringValue(18).equals(getStringValue(19)) && getStringValue(18).equals(getStringValue(20)));
        assertTrue(getStringValue(21).equals(getStringValue(22)) && getStringValue(21).equals(getStringValue(23)));
        String now = getStringValue(23);
        String now0 = getStringValue(24); // xxx.0的形式
        String now1 = getStringValue(25);
        String now2 = getStringValue(26);
        String now3 = getStringValue(27);
        String now4 = getStringValue(28);
        assertTrue(now0.length() == now1.length());
        assertTrue(now.length() >= now4.length() && now3.length() >= now2.length() && now2.length() >= now1.length());

        assertTrue(getStringValue(29).startsWith("2013-01-21"));
        assertEquals(-2, getIntValue(30));
        assertEquals(21, getIntValue(31));
        assertEquals(2012, getIntValue(32));

        assertEquals("Fri, 21 Dec 2012 03:59:59 GMT", getStringValue(33));
        assertTrue(getStringValue(34) == null);
        assertTrue(getStringValue(35) == null);
        assertEquals("2001-02-03 11:05:06.0", getStringValue(36));

        closeResultSet();

        sql = "SELECT d, t, ts FROM DateAndTimeFunctionTest";
        printResultSet();
    }

    @Override
    protected void test() throws Exception {
        // 以下有21个日期与时间函数,
        // 在这个方法中org.lealone.expression.Function.getSimpleValue(Session, Value, Expression[], Value[])
        // ----------------------------------------------------------------------------------------
        sql = "SELECT DAYNAME(DATE '2000-01-01')";
        sql = "SELECT DAY_OF_MONTH(CURRENT_DATE),DAY_OF_WEEK(CURRENT_DATE),DAY_OF_YEAR(CURRENT_DATE)";

        sql = "SELECT HOUR(CURRENT_TIMESTAMP),MINUTE(CURRENT_TIMESTAMP)";

        sql = "SELECT MONTH(CURRENT_DATE)"; // 加括号也可以SELECT MONTH(CURRENT_DATE())

        sql = "SELECT MONTHNAME(CURRENT_DATE)"; // 不是MONTH_NAME，没有下划线

        sql = "SELECT QUARTER(CURRENT_DATE)"; // 第几个季度，用1、2、3、4表示

        sql = "SELECT SECOND(CURRENT_TIMESTAMP)";

        sql = "SELECT WEEK(CURRENT_DATE),YEAR(CURRENT_DATE)";

        // ISO_DAY_OF_WEEK这个就正常了，周1用数字1表示，跟DAY_OF_WEEK不一样
        sql = "SELECT ISO_YEAR(CURRENT_DATE),ISO_WEEK(CURRENT_DATE),ISO_DAY_OF_WEEK(CURRENT_DATE)";

        // NOW(1)表示毫秒数只保留一位，如NOW()="2012-12-03 22:03:44.647" 则NOW(1)="2012-12-03 22:03:44.6"
        // 毫秒数一般是3位，如果NOW(100)，100>3了，所以NOW(100)跟NOW()一样
        sql = "SELECT CURDATE(),CURRENT_DATE(),CURTIME(),CURRENT_TIME(),NOW(),CURRENT_TIMESTAMP(),NOW(1),NOW(100)";

        // CURDATE=CURRENT_DATE, CURTIME=CURRENT_TIME, NOW=CURRENT_TIMESTAMP
        // 这个不加括号可以
        sql = "SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP";
        // 这个就不行
        // sql = "SELECT CURDATE, CURTIME, NOW";
        //
        //
        //
        // 以下有5个日期与时间函数,
        // 在这个方法中org.lealone.expression.Function.getValueWithArgs(Session, Expression[])
        // ----------------------------------------------------------------------------------------

        // 月份加1，结果是2001-02-28 00:00:00.0
        sql = "SELECT DATEADD('MONTH', 1, DATE '2001-01-31')";

        // 用后面的YEAR减去前面的YEAR，1999-2001=-2
        sql = "SELECT DATEDIFF('YEAR', DATE '2001-01-31', DATE '1999-01-31')";

        // 抽取日期和年份 CURRENT_TIMESTAMP=2012-12-03 22:20:08.597 DAY=3 YEAR=2012
        sql = "SELECT CURRENT_TIMESTAMP, EXTRACT(DAY FROM CURRENT_TIMESTAMP), EXTRACT(YEAR FROM CURRENT_TIMESTAMP)";

        // format datetime 格式化日期时间
        // timestamp = TIMESTAMP '2001-02-03 04:05:06' ,
        // formatString = 'EEE, d MMM yyyy HH:mm:ss z'
        // localeString = 'en'
        // timeZoneString = 'GMT'
        // 结果 Fri, 2 Feb 2001 20:05:06 GMT
        sql = "SELECT FORMATDATETIME(TIMESTAMP '2001-02-03 04:05:06', 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT')";
        // 只要timestamp和formatString其中之一为null，结果为null
        sql = "SELECT FORMATDATETIME(null, 'EEE, d MMM yyyy HH:mm:ss z')"; // null
        sql = "SELECT FORMATDATETIME(TIMESTAMP '2001-02-03 04:05:06', null)"; // null

        // parse datetime解析日期时间
        // 按后面三个参数指定的格式解析第一个参数，得到一个java.util.Date
        // 结果: 2001-02-03 11:05:06.0
        sql = "SELECT PARSEDATETIME('Sat, 3 Feb 2001 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT')";
        printResultSet();
    }

}
