/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.main;

import org.lealone.main.Lealone;

public class LealoneTest {

    public static void main(String[] args) {
        System.setProperty("lealone.config", "lealone-test.yaml");
        String[] args2 = { "-cluster", "1" };
        Lealone.main(args2);
    }

}
