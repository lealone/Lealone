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
package com.codefollower.yourbase.test.jdbc.function;

import static junit.framework.Assert.assertEquals;
import org.junit.Test;

import com.codefollower.yourbase.test.jdbc.TestBase;

//TODO H2总共支持14个聚合函数，还有group_concat、selectivity、histogram这三个没有实现
public class AggregateFunctionTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        testAggregateFunctions();
        testAggregateFunctionsWithGroupBy();
    }

    void init() throws Exception {
        //建立了4个分区
        //------------------------
        //分区1: rowKey < 25
        //分区2: 25 <= rowKey < 50
        //分区3: 50 <= rowKey < 75
        //分区4: rowKey > 75
        createTable("AggregateFunctionTest", "25", "50", "75");

        //在分区1中保存1到11中的奇数
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('01', 'a1', 'b', 1)");
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('02', 'a1', 'b', 3)");
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('03', 'a1', 'b', 5)");
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('04', 'a2', 'b', 7)");
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('05', 'a2', 'b', 9)");
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('06', 'a2', 'b', 11)");

        //分区2到4中分别保存1到11中的奇数
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('25', 'a1', 'b', 1)");
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('26', 'a1', 'b', 3)");

        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('50', 'a1', 'b', 5)");
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('51', 'a2', 'b', 7)");

        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('75', 'a2', 'b', 9)");
        stmt.executeUpdate("INSERT INTO AggregateFunctionTest(_rowkey_, f1, cf1.f2, cf2.f3) VALUES('76', 'a2', 'b', 11)");
    }

    //以1结尾的变量是用来统计分区1中的值;
    //以2结尾的变量是用来统计分区2到4中的值
    //单分区的统计跟H2数据库的算法一样，多分区统计需要另外的并行计算算法，
    //比如avg在统计多分区时会先转成count和sum，然后把count和sum发个多个分区，然后合并多个分区返回的结果，最后再算avg。
    //不过应用层不需要关心是否是单个还是多个分区，用什么算法来统计对应用是透明的。
    int count1, count2, max1, max2, min1, min2, sum1, sum2;
    boolean bool_and1, bool_and2, bool_or1, bool_or2;
    double avg1, avg2, stddev_pop1, stddev_pop2, stddev_samp1, stddev_samp2, var_pop1, var_pop2, var_samp1, var_samp2;

    String select = "SELECT count(*), max(cf2.f3), min(cf2.f3), sum(cf2.f3), " //

            + " bool_and((cf2.f3 % 2)=1), " //
            + " bool_or(cf2.f3=5), " //

            + " avg(cf2.f3), " //

            + " stddev_pop(cf2.f3), " //
            + " stddev_samp(cf2.f3), " //
            + " var_pop(cf2.f3), " //
            + " var_samp(cf2.f3) " //

            + " FROM AggregateFunctionTest WHERE ";

    void testAggregateFunctions() throws Exception {

        sql = select + "_rowkey_ >= '01' AND _rowkey_ < '25'";
        getValues1();

        sql = select + " _rowkey_ >= '25'";
        getValues2();

        assertValues();
    }

    void testAggregateFunctionsWithGroupBy() throws Exception {
        //会得到两个分组，一个是a1，另一个是a2

        //测试a1的分组
        sql = select + "_rowkey_ >= '01' AND _rowkey_ < '25' GROUP BY f1";
        getValues1();

        sql = select + " _rowkey_ >= '25' GROUP BY f1";
        getValues2();

        assertValues();

        //测试a2的分组
        sql = select + "_rowkey_ >= '01' AND _rowkey_ < '25' GROUP BY f1";
        next();
        getValues1();

        sql = select + " _rowkey_ >= '25' GROUP BY f1";
        next();
        getValues2();

        assertValues();
    }

    void getValues1() throws Exception {
        count1 = getIntValue(1);
        max1 = getIntValue(2);
        min1 = getIntValue(3);
        sum1 = getIntValue(4);

        bool_and1 = getBooleanValue(5);
        bool_or1 = getBooleanValue(6);

        avg1 = getDoubleValue(7);

        stddev_pop1 = getDoubleValue(8);
        stddev_samp1 = getDoubleValue(9);
        var_pop1 = getDoubleValue(10);
        var_samp1 = getDoubleValue(11, true);
    }

    void getValues2() throws Exception {
        count2 = getIntValue(1);
        max2 = getIntValue(2);
        min2 = getIntValue(3);
        sum2 = getIntValue(4);

        bool_and2 = getBooleanValue(5);
        bool_or2 = getBooleanValue(6);

        avg2 = getDoubleValue(7);

        stddev_pop2 = getDoubleValue(8);
        stddev_samp2 = getDoubleValue(9);
        var_pop2 = getDoubleValue(10);
        var_samp2 = getDoubleValue(11, true);
    }

    void assertValues() {
        assertEquals(count1, count2);
        assertEquals(max1, max2);
        assertEquals(min1, min2);
        assertEquals(sum1, sum2);

        assertEquals(bool_and1, bool_and2);
        assertEquals(bool_or1, bool_or2);

        assertEquals(avg1, avg2, 0.00000001);

        //单分区求值使用的是H2数据库自身的算法，所以跟多分区时使用的算法不一样，H2数据库自身的算法会有更多误差
        //H2数据库自身的算法见: http://www.johndcook.com/standard_deviation.html
        assertEquals(stddev_pop1, stddev_pop2, 0.00000001);
        assertEquals(stddev_samp1, stddev_samp2, 0.00000001);
        //assertEquals(var_pop1, var_pop2, 0.0000000000000000000001); //比如这个就会出错
        assertEquals(var_pop1, var_pop2, 0.00000001);
        assertEquals(var_samp1, var_samp2, 0.00000001);
    }

}
