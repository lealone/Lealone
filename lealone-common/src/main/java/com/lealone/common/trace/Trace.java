/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.common.trace;

import java.util.ArrayList;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.CommandParameter;
import com.lealone.db.value.Value;

/**
 * This class represents a trace module.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface Trace {

    int getTraceId();

    String getTraceObjectName();

    Trace setType(TraceModuleType type);

    /**
     * Check if the trace level is equal or higher than INFO.
     *
     * @return true if it is
     */
    boolean isInfoEnabled();

    /**
     * Check if the trace level is equal or higher than DEBUG.
     *
     * @return true if it is
     */
    boolean isDebugEnabled();

    /**
     * Write a message with trace level ERROR to the trace system.
     *
     * @param t the exception
     * @param s the message
     */
    void error(Throwable t, String s);

    /**
     * Write a message with trace level ERROR to the trace system.
     *
     * @param t the exception
     * @param s the message
     * @param params the parameters
     */
    void error(Throwable t, String s, Object... params);

    /**
     * Write a message with trace level INFO to the trace system.
     *
     * @param s the message
     */
    void info(String s);

    /**
     * Write a message with trace level INFO to the trace system.
     *
     * @param s the message
     * @param params the parameters
     */
    void info(String s, Object... params);

    /**
     * Write a message with trace level INFO to the trace system.
     *
     * @param t the exception
     * @param s the message
     */
    void info(Throwable t, String s);

    /**
     * Write a SQL statement with trace level INFO to the trace system.
     *
     * @param sql the SQL statement
     * @param params the parameters used, in the for {1:...}
     * @param count the update count
     * @param time the time it took to run the statement in ms
     */
    void infoSQL(String sql, String params, int count, long time);

    /**
     * Write Java source code with trace level INFO to the trace system.
     *
     * @param java the source code
     */
    void infoCode(String format, Object... args);

    /**
     * Write a message with trace level DEBUG to the trace system.
     *
     * @param s the message
     */
    void debug(String s);

    /**
     * Write a message with trace level DEBUG to the trace system.
     *
     * @param s the message
     * @param params the parameters
     */
    void debug(String s, Object... params);

    /**
     * Write a message with trace level DEBUG to the trace system.
     * @param t the exception
     * @param s the message
     */
    void debug(Throwable t, String s);

    /**
     * Write Java source code with trace level DEBUG to the trace system.
     *
     * @param java the source code
     */
    void debugCode(String java);

    /**
     * Format the parameter list.
     *
     * @param parameters the parameter list
     * @return the formatted text
     */
    public static String formatParams(ArrayList<? extends CommandParameter> parameters) {
        if (parameters.isEmpty()) {
            return "";
        }
        StatementBuilder buff = new StatementBuilder();
        int i = 0;
        boolean params = false;
        for (CommandParameter p : parameters) {
            if (p.isValueSet()) {
                if (!params) {
                    buff.append(" {");
                    params = true;
                }
                buff.appendExceptFirst(", ");
                Value v = p.getValue();
                buff.append(++i).append(": ").append(v.getTraceSQL());
            }
        }
        if (params) {
            buff.append('}');
        }
        return buff.toString();
    }

    public static final NoTrace NO_TRACE = new NoTrace();
}
