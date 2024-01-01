/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.postgresql;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class PgJdbcTest extends PgTestBase {
    @Test
    public void run() throws Exception {
        try {
            Statement statement = conn.createStatement();
            statement.executeUpdate("drop table if exists pet");
            statement.executeUpdate("create table if not exists pet(name varchar(20), age int)");
            statement.executeUpdate("insert into pet values('pet1', 2)");

            ResultSet rs = statement.executeQuery("select count(*) from pet");
            rs.next();
            System.out.println("Statement.executeQuery count: " + rs.getInt(1));
            rs.close();

            String sql = "select name, age from pet where name=?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, "pet1");
            rs = ps.executeQuery();
            rs.next();
            String name = rs.getString(1);
            int age = rs.getInt(2);
            System.out.println("PreparedStatement.executeQuery name: " + name + ", age: " + age);

            rs.close();
            ps.setString(1, "pet2");
            rs = ps.executeQuery();
            assertFalse(rs.next());

            // 执行非Prepared语句每次都要发5个请求包: Parse Bind Describe Execute Sync
            // 重复执行Prepared语句，默认前5次依然发5个请求包，从第6次执行开始减少到3个请求包: Bind Execute Sync
            // for (int i = 0; i < 10; i++) {
            // rs.close();
            // ps.setString(1, "pet1");
            // rs = ps.executeQuery();
            // rs.next();
            // // rs = statement.executeQuery("select * from pet");
            // }

            ParameterMetaData pmd = ps.getParameterMetaData();
            pmd.getParameterCount();
            pmd.getParameterTypeName(1);
            rs.close();
            ps.close();
            statement.close();
        } catch (SQLException e) {
            sqlException(e);
            e.printStackTrace();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
