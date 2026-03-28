/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lealone.agent.doubao.DoubaoAgent;
import com.lealone.db.util.SourceCompiler;

public class LealoneAgent {

    public static void main(String[] args) throws Exception {
        promptLoop(args);
        // run(args, null);
    }

    private static String extractClassName(String code) {
        Pattern pattern = Pattern.compile("class\\s+(\\w+)");
        Matcher matcher = pattern.matcher(code);
        if (matcher.find())
            return matcher.group(1);
        throw new IllegalArgumentException("无法找到类名");
    }

    private static void promptLoop(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("prompt>");
            String line = readLine(reader);
            if (line == null) {
                break;
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            run(args, trimmed);
        }
    }

    private static String readLine(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("Aborted");
        }
        return line;
    }

    public static void run(String[] args, String userPrompt) throws Exception {
        userPrompt = userPrompt != null ? userPrompt : "写一个hello world";
        // userPrompt = "输出当前日期";
        DoubaoAgent agent = new DoubaoAgent();
        agent.init(args);
        String javaCode = agent.generateJavaCode(userPrompt);
        // System.out.println(javaCode);
        // String javaCode = """
        // public class HelloWorld {
        // public static void main(String[] args) {
        // System.out.println("Hello World!");
        // }
        // }""";

        Class<?> clz = SourceCompiler.compileAsClass(extractClassName(javaCode), javaCode);
        clz.getMethod("main", String[].class).invoke(null, (Object) args);
    }
}
