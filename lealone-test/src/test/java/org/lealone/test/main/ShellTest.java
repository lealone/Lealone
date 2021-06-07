/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.main;

import java.sql.SQLException;

import org.lealone.main.Shell;

//public class ShellTest extends TestBase {
//
//    public static void main(String[] args) throws SQLException {
//        ShellTest test = new ShellTest();
//        test.run();
//    }
//
//    void run() throws SQLException {
//        String url = getURL(LealoneDatabase.NAME);
//        String[] args = { "-url", url };
//        Shell.main(args);
//    }
//}

public class ShellTest {

    public static void main(String[] args) throws SQLException {
        System.setProperty("lealone.config", "lealone-test.yaml");
        String url = "jdbc:lealone:tcp://localhost:9210/lealone";
        // url = "jdbc:lealone:embed:lealone";
        String[] args2 = { "-url", url, "-user", "root" };
        Shell.main(args2);
    }
}
