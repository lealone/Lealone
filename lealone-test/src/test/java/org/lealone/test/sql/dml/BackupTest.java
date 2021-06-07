/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.dml;

import org.lealone.test.TestBase.TodoTest;
import org.lealone.test.sql.SqlTestBase;

public class BackupTest extends SqlTestBase implements TodoTest {
    // @Test
    public void run() {
        executeUpdate("drop table IF EXISTS BackupTest");
        executeUpdate("create table IF NOT EXISTS BackupTest(id int, name varchar(500), b boolean)");
        executeUpdate("CREATE INDEX IF NOT EXISTS BackupTestIndex ON BackupTest(name)");

        executeUpdate("insert into BackupTest(id, name, b) values(1, 'a1', true)");
        executeUpdate("insert into BackupTest(id, name, b) values(1, 'b1', true)");
        executeUpdate("insert into BackupTest(id, name, b) values(2, 'a2', false)");
        executeUpdate("insert into BackupTest(id, name, b) values(2, 'b2', true)");
        executeUpdate("insert into BackupTest(id, name, b) values(3, 'a3', false)");
        executeUpdate("insert into BackupTest(id, name, b) values(3, 'b3', true)");

        sql = "BACKUP TO " + joinDirs("myBackup.zip"); // 文件名要加单引号
        sql = "BACKUP TO '" + joinDirs("myBackup.zip") + "'";
        executeUpdate(sql);

        sql = "select * from BackupTest";
        printResultSet();
    }
}
