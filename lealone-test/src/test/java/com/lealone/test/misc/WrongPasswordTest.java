/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import com.lealone.db.LealoneDatabase;
import com.lealone.test.TestBase;

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
