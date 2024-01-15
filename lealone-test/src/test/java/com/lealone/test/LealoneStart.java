/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test;

import com.lealone.main.Lealone;

public class LealoneStart {

    public static void main(String[] args) {
        Lealone.main(args);
    }

    public static void run() {
        Lealone.main(new String[0], null);
    }
}
