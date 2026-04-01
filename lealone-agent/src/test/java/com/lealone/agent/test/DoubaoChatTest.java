/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent.test;

import com.lealone.agent.doubao.DoubaoAgent;

public class DoubaoChatTest extends DoubaoAgent {

    public static void main(String[] args) {
        new DoubaoChatTest().run(args);
    }

    public void run(String[] args) {
        init(args);
        System.out.println(generateJavaCode("写一个hello world"));
    }
}
