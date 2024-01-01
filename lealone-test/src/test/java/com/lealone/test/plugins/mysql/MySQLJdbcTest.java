/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class MySQLJdbcTest extends MySQLTestBase {
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
            statement.close();

            String sql = "select name, age from pet where name=?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, "pet1");
            rs = ps.executeQuery();
            rs.next();
            String name = rs.getString(1);
            int age = rs.getInt(2);
            System.out.println("PreparedStatement.executeQuery name: " + name + ", age: " + age);
            rs.close();
            ps.close();
        } catch (SQLException e) {
            sqlException(e);
            e.printStackTrace();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
