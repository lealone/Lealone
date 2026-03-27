package com.lealone.agent.test;
/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */


import com.lealone.agent.llm.doubao.DoubaoLLM;

public class DoubaoChatTest extends DoubaoLLM {

    public static void main(String[] args) {
        new DoubaoChatTest().run(args);
    }

    public void run(String[] args) {
        init(args);
        System.out.println(generateJavaCode("写一个hello world"));
    }
}
