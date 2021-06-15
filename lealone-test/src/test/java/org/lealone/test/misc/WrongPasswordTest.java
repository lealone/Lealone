/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;

public class WrongPasswordTest {

    public static void main(String[] args) {
        TestBase test = new TestBase();
        test.setUser("xxx");
        try {
            test.getConnection(LealoneDatabase.NAME);
        } catch (Exception e) {
            test = new TestBase();
            test.setPassword("yyy");
            try {
                test.getConnection(LealoneDatabase.NAME);
            } catch (Exception e2) {
                test = new TestBase();
                try {
                    test.getConnection(LealoneDatabase.NAME);
                } catch (Exception e3) {
                }
            }
        }
    }
}
