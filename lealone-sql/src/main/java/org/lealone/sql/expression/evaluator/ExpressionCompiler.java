/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.evaluator;

import java.util.TreeSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.ServerSession;
import org.lealone.db.util.SourceCompiler;
import org.lealone.sql.expression.Expression;

public class ExpressionCompiler {

    private static long id;

    public static synchronized JitEvaluator createJitEvaluator(ServerSession session, Expression expression) {
        StringBuilder body = new StringBuilder();
        TreeSet<String> importSet = new TreeSet<>();
        importSet.add(JitEvaluator.class.getName());
        expression.genCode(body, importSet);

        id++;
        String className = "JitEvaluator" + id;
        StringBuilder buff = new StringBuilder();
        buff.append("package ").append(Constants.USER_PACKAGE).append(".expression.evaluator").append(";\r\n\r\n");
        for (String p : importSet) {
            buff.append("import ").append(p).append(";\r\n");
        }
        buff.append("\r\n");
        buff.append("public class ").append(className).append(" extends JitEvaluator").append(" {\r\n");
        buff.append("    @Override").append("\r\n");
        buff.append("    public boolean getBooleanValue() {").append("\r\n");
        buff.append("        return true;").append("\r\n");
        buff.append("    }").append("\r\n");
        buff.append("}\r\n");

        String fullClassName = Constants.USER_PACKAGE + ".expression.evaluator." + className;
        return loadFromSource(session, buff, fullClassName);
    }

    private static JitEvaluator loadFromSource(ServerSession session, StringBuilder buff, String fullClassName) {
        // SourceCompiler compiler = session.getDatabase().getCompiler();
        // compiler.setSource(fullClassName, buff.toString());
        // try {
        // return (JitEvaluator) compiler.getClass(fullClassName).getDeclaredConstructor().newInstance();
        // } catch (Exception e) {
        // throw DbException.convert(e);
        // }

        try {
            return (JitEvaluator) SourceCompiler.compileAsClass(fullClassName, buff.toString()).getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public static synchronized void createJitEvaluatorAsync(ServerSession session, Expression expression,
            AsyncHandler<AsyncResult<JitEvaluator>> asyncHandler) {
        id++;
        Thread t = new Thread(() -> {
            try {
                JitEvaluator e = createJitEvaluator(session, expression);
                asyncHandler.handle(new AsyncResult<>(e));
            } catch (Exception e) {
                asyncHandler.handle(new AsyncResult<>(e));
            }
        });
        t.setName("AsyncCreateJitEvaluatorThread-" + id);
        t.start();
    }
}
