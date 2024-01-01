/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import com.lealone.db.util.SourceCompiler;

public class SourceCompilerTest {

    public static void main(String[] args) throws Exception {
        String name = SourceCompilerTest.class.getName();
        String str = "public class Test { " + name + " f; public void m() {" + name + ".test(); }}";

        Class<?> clz = SourceCompiler.compileAsClass("Test", str);
        Object obj = clz.getDeclaredConstructor().newInstance();
        clz.getDeclaredMethod("m").invoke(obj);

        obj = SourceCompiler.compileAsInstance("Test", str);
        obj.getClass().getDeclaredMethod("m").invoke(obj);
    }

    public static void test() {
        System.out.println("test");
    }
}
