/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.main;

import org.lealone.main.Lealone;

public class LealoneTest {

    public static void main(String[] args) {
        // System.setProperty("lealone.config", "lealone-test.yaml");
        String[] args2 = { //
                // "-help", //
                // "-cluster", //
                // "-seeds", "127.0.0.2", //
                // "-host", "127.0.0.1", //
                // "-port", "9000", //
                // "-config", "lealone-test.yaml",//
        };
        Lealone.main(args2);
    }

}
