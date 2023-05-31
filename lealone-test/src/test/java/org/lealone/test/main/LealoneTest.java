/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.main;

import org.lealone.main.Lealone;

public class LealoneTest {

    public static void main(String[] args) {
        String[] args2 = { //
                // "-help", //
                // "-host", "127.0.0.1", //
                // "-port", "9000", //
                // "-config", "lealone-test.yaml",//
        };
        Lealone.main(args2);
    }

}
