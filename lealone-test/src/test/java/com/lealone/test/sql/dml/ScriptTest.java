/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.dml;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class ScriptTest extends SqlTestBase {

    public ScriptTest() {
        super("ScriptTestDB"); // 连接到不同的数据库测试，避免受其他测试影响
    }

    @Test
    public void run() {
        executeUpdate("create table IF NOT EXISTS ScriptTest(id int)");
        // 生成各种Create SQL，此命令返回结果集，所以要用executeQuery
        sql = "SCRIPT SIMPLE NODATA NOPASSWORDS NOSETTINGS TO 'my_script_test.sql' TABLE ScriptTest";
        printResultSet();

        executeUpdate("drop table ScriptTest");
        executeUpdate("RUNSCRIPT FROM 'my_script_test.sql'");

        executeUpdate("drop table ScriptTest");
        String fileName = "./target/test-data/script_directory/my_script_test.sql";
        executeUpdate("RUNSCRIPT FROM '" + fileName + "'");
    }
}
