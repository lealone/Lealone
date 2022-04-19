/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.orm;

import org.lealone.test.TestBase.MainTest;
import org.lealone.test.TestBase.SqlExecutor;
import org.lealone.test.service.ExecuteServiceTest;
import org.lealone.test.service.impl.AllTypeServiceImpl;
import org.lealone.test.service.impl.HelloWorldServiceImpl;
import org.lealone.test.service.impl.UserServiceImpl;

public class SqlScript implements MainTest {

    public static void main(String[] args) {
        new SqlScriptTest().runTest();
    }

    private static class SqlScriptTest extends OrmTestBase {
        @Override
        public void test() {
            createCustomerTable(this);
            createCustomerAddressTable(this);
            createUserTable(this);
            createProductTable(this);
            createOrderTable(this);
            createOrderItemTable(this);
            createAllModelPropertyTable(this);
            createUserService(this);
            createHelloWorldService(this);
            createAllTypeService(this);
        }
    }

    private static final String MODEL_PACKAGE_NAME = OrmTest.class.getPackage().getName() + ".generated";
    private static final String SERVICE_PACKAGE_NAME = ExecuteServiceTest.class.getPackage().getName() + ".generated";
    private static String GENERATED_CODE_PATH = "./src/test/java";

    public static void setCodePath(String path) {
        GENERATED_CODE_PATH = path;
    }

    public static void createTables(SqlExecutor executor) {
        createCustomerTable(executor);
        createCustomerAddressTable(executor);
        createUserTable(executor);
        createProductTable(executor);
        createOrderTable(executor);
        createOrderItemTable(executor);
        createAllModelPropertyTable(executor);
    }

    public static void createUserTable(SqlExecutor executor) {
        System.out.println("create table: user");
        executor.execute("drop table if exists user");
        // 创建表: user
        executor.execute(
                "create table if not exists user(name char(10) primary key, notes varchar, phone int, id long, phones ARRAY)" //
                        + " package '" + MODEL_PACKAGE_NAME + "'" //
                        + " generate code '" + GENERATED_CODE_PATH + "'");
    }

    public static void createCustomerTable(SqlExecutor executor) {
        System.out.println("create table: customer");

        executor.execute("drop table if exists customer");
        executor.execute(
                "create table if not exists customer(id long primary key, name char(10), notes varchar, phone int)" //
                        + " package '" + MODEL_PACKAGE_NAME + "'" //
                        + " generate code '" + GENERATED_CODE_PATH + "'" // 生成领域模型类和查询器类的代码
        );
    }

    public static void createCustomerAddressTable(SqlExecutor executor) {
        System.out.println("create table: customer_address");

        executor.execute("drop table if exists customer_address");
        executor.execute("create table if not exists customer_address(customer_id long, city varchar, street varchar, "
                + " FOREIGN KEY(customer_id) REFERENCES customer(id))" //
                + " package '" + MODEL_PACKAGE_NAME + "'" //
                + " generate code '" + GENERATED_CODE_PATH + "'" //
        );
    }

    public static void createProductTable(SqlExecutor executor) {
        System.out.println("create table: product");

        executor.execute("drop table if exists product");
        executor.execute("create table if not exists product(product_id long primary key, product_name varchar, "
                + " category varchar, unit_price double)" //
                + " package '" + MODEL_PACKAGE_NAME + "'" //
                + " generate code '" + GENERATED_CODE_PATH + "'" // 生成领域模型类和查询器类的代码
        );
    }

    public static void createOrderTable(SqlExecutor executor) {
        System.out.println("create table: order");

        executor.execute("drop table if exists `order`");
        // order是关键字，所以要用特殊方式表式
        executor.execute(
                "create table if not exists `order`(customer_id long, order_id int primary key, order_date date, total double,"
                        + " FOREIGN KEY(customer_id) REFERENCES customer(id))" //
                        + " package '" + MODEL_PACKAGE_NAME + "'" //
                        + " generate code '" + GENERATED_CODE_PATH + "'" // 生成领域模型类和查询器类的代码
        );
    }

    public static void createOrderItemTable(SqlExecutor executor) {
        System.out.println("create table: order_item");

        executor.execute("drop table if exists order_item");
        executor.execute("create table if not exists order_item(order_id int, product_id long, product_count int, "
                + " FOREIGN KEY(order_id) REFERENCES `order`(order_id)," //
                + " FOREIGN KEY(product_id) REFERENCES product(product_id))" //
                + " package '" + MODEL_PACKAGE_NAME + "'" //
                + " generate code '" + GENERATED_CODE_PATH + "'" // 生成领域模型类和查询器类的代码
        );
    }

    // 21种模型属性类型，目前不支持GEOMETRY类型
    // INT
    // BOOLEAN
    // TINYINT
    // SMALLINT
    // BIGINT
    // IDENTITY
    // DECIMAL
    // DOUBLE
    // REAL
    // TIME
    // DATE
    // TIMESTAMP
    // BINARY
    // OTHER
    // VARCHAR
    // VARCHAR_IGNORECASE
    // CHAR
    // BLOB
    // CLOB
    // UUID
    // ARRAY
    public static String TEST_TYPES = "" //
            + " f1  INT," //
            + " f2  BOOLEAN," //
            + " f3  TINYINT," //
            + " f4  SMALLINT," //
            + " f5  BIGINT," //
            + " f6  IDENTITY," //
            + " f7  DECIMAL," //
            + " f8  DOUBLE," //
            + " f9  REAL," //
            + " f10 TIME," //
            + " f11 DATE," //
            + " f12 TIMESTAMP," //
            + " f13 BINARY," //
            + " f14 OTHER," //
            + " f15 VARCHAR," //
            + " f16 VARCHAR_IGNORECASE," //
            + " f17 CHAR," //
            + " f18 BLOB," //
            + " f19 CLOB," //
            + " f20 UUID," //
            + " f21 ARRAY" //
    ;

    public static void createAllModelPropertyTable(SqlExecutor executor) {

        executor.execute("drop table if exists all_model_property");
        executor.execute("CREATE TABLE if not exists all_model_property (" //
                + TEST_TYPES + ")" //
                + " PACKAGE '" + MODEL_PACKAGE_NAME + "'" //
                + " GENERATE CODE '" + GENERATED_CODE_PATH + "'");

        System.out.println("create table: all_model_property");
    }

    public static void createAllTypeService(SqlExecutor executor) {
        System.out.println("create service: all_type_service");
        executor.execute("drop service if exists all_type_service");
        // 创建服务: all_type_service
        executor.execute("create service if not exists all_type_service (" //
                + " test_type(" + TEST_TYPES + ") user," //
                + " test_uuid(f1 uuid) uuid" //
                + ")" //
                + " package '" + SERVICE_PACKAGE_NAME + "'" //
                // 如果是内部类，不能用getClassName()，会包含$字符
                + " implement by '" + AllTypeServiceImpl.class.getCanonicalName() + "'" //
                + " generate code '" + GENERATED_CODE_PATH + "'");
    }

    public static void createUserService(SqlExecutor executor) {
        System.out.println("create service: user_service");
        executor.execute("drop service if exists user_service");
        // 创建服务: user_service
        executor.execute("create service if not exists user_service (" //
                + " add(user user) long," // 第一个user是参数名，第二个user是参数类型
                + " find(name varchar) user," //
                + " update(user user) int," //
                + " delete(name varchar) int)" //
                + " package '" + SERVICE_PACKAGE_NAME + "'" //
                // 如果是内部类，不能用getClassName()，会包含$字符
                + " implement by '" + UserServiceImpl.class.getCanonicalName() + "'" //
                + " generate code '" + GENERATED_CODE_PATH + "'");
    }

    public static void createHelloWorldService(SqlExecutor executor) {
        System.out.println("create service: hello_world_service");
        executor.execute("drop service if exists hello_world_service");
        // 创建服务: hello_world_service
        executor.execute("create service hello_world_service (" //
                + "             say_hello() void," //
                + "             get_date() date," //
                + "             get_int() int," //
                + "             get_two(name varchar, age int) int," //
                + "             say_goodbye_to(name varchar) varchar" //
                + "         ) package '" + SERVICE_PACKAGE_NAME + "'" //
                + "           implement by '" + HelloWorldServiceImpl.class.getName() + "'" //
                + "           generate code '" + GENERATED_CODE_PATH + "'");
    }
}
