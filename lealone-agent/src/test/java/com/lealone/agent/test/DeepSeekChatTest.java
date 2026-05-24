/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent.test;

import com.lealone.agent.provider.DeepSeekCodeAgent;

public class DeepSeekChatTest {

    public static void main(String[] args) {
        new DeepSeekChatTest().run(args);
    }

    public void run(String[] args) {
        DeepSeekCodeAgent agent = new DeepSeekCodeAgent();
        agent.init(null);
        System.out.println(agent.send("你是谁", null));
    }
}
