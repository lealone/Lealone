/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.expression;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ExpressionTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        // stmt.executeUpdate("create table IF NOT EXISTS mytable(id int primary key, name varchar(500))");

        // stmt.executeUpdate("SET @topVariableName=3");
        // stmt.executeUpdate("delete top @topVariableName from mytable where id>10");
        // stmt.executeUpdate("delete top 3 from mytable where id>10");
        // stmt.executeUpdate("delete top ?1 from mytable where id>10");

        // stmt.executeUpdate("delete top true from mytable where id>10");
        // stmt.executeUpdate("update mytable set name='1234567890' where id>10");
        // stmt.executeUpdate("delete top rownum from mytable where id>10");
        // stmt.executeUpdate("delete top CURRENT_TIME from mytable where id>10");
        // stmt.executeUpdate("delete top MAX(id) from mytable where id>10");

        // stmt.executeUpdate("select MAX(id) from mytable where id>10");

        // ps =conn.prepareStatement("delete top ?2 from mytable where id>10");

        // PreparedStatement ps =conn.prepareStatement("delete top ?2 from mytable where id>10 and name=?1");
        // ps.setString(1, "abcdef1234");
        // ps.setInt(2, 3);
        // ps.executeUpdate();

        // 下面2条SQL测试org.h2.command.Parser.readCondition()中的NOT和EXISTS
        // stmt.executeUpdate("delete from mytable where not (id>0)");
        // stmt.executeUpdate("delete from mytable where EXISTS (select id from mytable where id>10)");

        // 下面5条SQL测试org.h2.command.Parser.readConcat()的5种情况
        // stmt.executeUpdate("delete from mytable where name='aaa' || 'bbb' || 'ccc'");
        // stmt.executeUpdate("delete from mytable where name~'aaa'");
        // stmt.executeUpdate("delete from mytable where name~*'aaa'");
        // stmt.executeUpdate("delete from mytable where name!~'aaa'");
        // stmt.executeUpdate("delete from mytable where name!~*'aaa'");

        // stmt.executeUpdate("delete from mytable where name not null"); // 这样不合法
        // stmt.executeUpdate("delete from mytable where name is null"); //这样才合法

        // stmt.executeUpdate("delete from mytable where name like 'abc' ESCAPE 'bcd'"); //ESCAPE只能是一个字符
        // stmt.executeUpdate("delete from mytable where name like 'abc' ESCAPE 'b'");
        // stmt.executeUpdate("delete from mytable where name REGEXP 'b-'");

        // stmt.executeUpdate("delete from mytable where name is not null");
        // stmt.executeUpdate("delete from mytable where name is not DISTINCT FROM 'abc'");
        // stmt.executeUpdate("delete from mytable where name is not 'abc'");

        // stmt.executeUpdate("delete from mytable where name is null");
        // stmt.executeUpdate("delete from mytable where name is DISTINCT FROM 'abc'");
        // stmt.executeUpdate("delete from mytable where name is 'abc'");

        // stmt.executeUpdate("delete from mytable where name in()");
        // stmt.executeUpdate("delete from mytable where name in(select name from mytable where id>10)");
        // stmt.executeUpdate("delete from mytable where id in(1,2,4)");

        // stmt.executeUpdate("delete from mytable where id BETWEEN 1 AND 2");

        // stmt.executeUpdate("delete from mytable where id > ALL(select id from mytable where id>10)");
        // // ANY和SOME一样
        // stmt.executeUpdate("delete from mytable where id > ANY(select id from mytable where id>10)");
        // stmt.executeUpdate("delete from mytable where id > SOME(select id from mytable where id>10)");
        // // stmt.executeUpdate("delete from mytable where id > 10 (+) id > 10");
        // // stmt.executeUpdate("delete from mytable where id > id (+)");
        // stmt.executeUpdate("delete from mytable where id > 10");
        //
        // for (int i = 1; i <= 200; i++) {
        // stmt.executeUpdate("insert into mytable(id, name) values(" + i + ", 'abcdef1234')");
        // }
    }
}
