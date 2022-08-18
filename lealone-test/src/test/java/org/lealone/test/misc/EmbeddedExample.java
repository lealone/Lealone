/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import java.sql.Connection;
import java.sql.DriverManager;

import org.lealone.common.trace.TraceSystem;
import org.lealone.db.DbSetting;
import org.lealone.db.SysProperties;
import org.lealone.test.TestBase;

public class EmbeddedExample {

    public static void main(String[] args) throws Exception {
        SysProperties.setBaseDir(TestBase.TEST_DIR);

        String url = "jdbc:lealone:embed:embedded_db?" + DbSetting.TRACE_LEVEL_FILE + "="
                + TraceSystem.DEBUG;
        Connection conn = DriverManager.getConnection(url, "root", "");
        CRUDExample.crud(conn);

        // 数据库名在内部会对应一个ID，目录名也是用ID表示，所以my/embedded_db跟embedded_db是一样的
        url = "jdbc:lealone:embed:my/embedded_db";
        conn = DriverManager.getConnection(url, "root", "");
        CRUDExample.crud(conn);

        url = "jdbc:lealone:embed:embedded_memory_db?" + DbSetting.PERSISTENT + "=false";
        conn = DriverManager.getConnection(url, "root", "");
        CRUDExample.crud(conn);
    }
}
