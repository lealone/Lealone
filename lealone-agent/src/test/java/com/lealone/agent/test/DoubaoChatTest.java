/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent.test;

import java.util.concurrent.atomic.AtomicReference;

import com.lealone.agent.provider.DoubaoCodeAgent;

public class DoubaoChatTest {

    public static void main(String[] args) {
        new DoubaoChatTest().run(args);
    }

    public void run(String[] args) {
        DoubaoCodeAgent agent = new DoubaoCodeAgent();
        agent.init(null);
        AtomicReference<String> previousResponseId = new AtomicReference<>();
        System.out.println(agent.send("你是谁", previousResponseId));
        System.out.println(agent.send("我刚才说了什么", previousResponseId));
    }
}
