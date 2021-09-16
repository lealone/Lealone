/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.function;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

//TODO H2总共支持14个聚合函数，还有group_concat、selectivity、histogram这三个没有实现
public class AggregateFunctionTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        testAggregateFunctions();
        testAggregateFunctionsWithGroupBy();
        testHistogram();
    }

    void init() throws Exception {
        createTable("AggregateFunctionTest");

        // 在分区1中保存1到11中的奇数
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', 1)");
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 3)");
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 5)");
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('04', 'a2', 'b', 7)");
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('05', 'a2', 'b', 9)");
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('06', 'a2', 'b', 11)");

        // 分区2到4中分别保存1到11中的奇数
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('25', 'a1', 'b', 1)");
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('26', 'a1', 'b', 3)");

        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('50', 'a1', 'b', 5)");
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('51', 'a2', 'b', 7)");

        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('75', 'a2', 'b', 9)");
        executeUpdate("INSERT INTO AggregateFunctionTest(pk, f1, f2, f3) VALUES('76', 'a2', 'b', 11)");
    }

    // 以1结尾的变量是用来统计分区1中的值;
    // 以2结尾的变量是用来统计分区2到4中的值
    // 单分区的统计跟H2数据库的算法一样，多分区统计需要另外的并行计算算法，
    // 比如avg在统计多分区时会先转成count和sum，然后把count和sum发个多个分区，然后合并多个分区返回的结果，最后再算avg。
    // 不过应用层不需要关心是否是单个还是多个分区，用什么算法来统计对应用是透明的。
    int count1, count2, max1, max2, min1, min2, sum1, sum2;
    boolean bool_and1, bool_and2, bool_or1, bool_or2;
    double avg1, avg2, stddev_pop1, stddev_pop2, stddev_samp1, stddev_samp2, var_pop1, var_pop2, var_samp1, var_samp2;

    String select = "SELECT count(*), max(f3), min(f3), sum(f3), " //

            + " bool_and((f3 % 2)=1), " //
            + " bool_or(f3=5), " //

            + " avg(f3), " //

            + " stddev_pop(f3), " //
            + " stddev_samp(f3), " //
            + " var_pop(f3), " //
            + " var_samp(f3) " //

            + " FROM AggregateFunctionTest WHERE ";

    void testAggregateFunctions() throws Exception {

        sql = select + "pk >= '01' AND pk < '25'";
        getValues1();

        sql = select + " pk >= '25'";
        getValues2();

        assertValues();
    }

    void testAggregateFunctionsWithGroupBy() throws Exception {
        // 会得到两个分组，一个是a1，另一个是a2

        // 测试a1的分组
        sql = select + "pk >= '01' AND pk < '25' GROUP BY f1";
        getValues1();

        sql = select + " pk >= '25' GROUP BY f1";
        getValues2();

        assertValues();

        // 测试a2的分组
        sql = select + "pk >= '01' AND pk < '25' GROUP BY f1";
        next();
        getValues1();

        sql = select + " pk >= '25' GROUP BY f1";
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

        // 单分区求值使用的是H2数据库自身的算法，所以跟多分区时使用的算法不一样，H2数据库自身的算法会有更多误差
        // H2数据库自身的算法见: http://www.johndcook.com/standard_deviation.html
        assertEquals(stddev_pop1, stddev_pop2, 0.00000001);
        assertEquals(stddev_samp1, stddev_samp2, 0.00000001);
        // assertEquals(var_pop1, var_pop2, 0.0000000000000000000001); //比如这个就会出错
        assertEquals(var_pop1, var_pop2, 0.00000001);
        assertEquals(var_samp1, var_samp2, 0.00000001);
    }

    void testHistogram() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS HistogramTest");
        stmt.executeUpdate("create table IF NOT EXISTS HistogramTest(name varchar, f1 int,f2 int)");

        stmt.executeUpdate("insert into HistogramTest values('abc',1,2)");
        stmt.executeUpdate("insert into HistogramTest values('abc',2,2)");
        stmt.executeUpdate("insert into HistogramTest values('abc',3,2)");
        stmt.executeUpdate("insert into HistogramTest values('abc',1,2)");
        stmt.executeUpdate("insert into HistogramTest values('abc',2,2)");
        stmt.executeUpdate("insert into HistogramTest values('abc',3,2)");

        // 加不加distinct都一样
        sql = "select HISTOGRAM(f1) from HistogramTest";
        assertEquals("((1, 2), (2, 2), (3, 2))", getStringValue(1, true));
        sql = "select HISTOGRAM(distinct f1) from HistogramTest";
        assertEquals("((1, 2), (2, 2), (3, 2))", getStringValue(1, true));
    }
}
