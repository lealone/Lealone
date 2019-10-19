/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.common.trace;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;

/**
 * The base class for objects that can print trace information about themselves.
 * 
 * @author H2 Group
 * @author zhh
 */
// 如果不想每次都用"trace.XXX"这样繁琐的风格使用Trace的API，可以继承这类个
public class TraceObject {

    private static final AtomicInteger[] ID;

    static {
        int length = TraceObjectType.values().length;
        ID = new AtomicInteger[length];
        for (int i = 0; i < length; i++) {
            ID[i] = new AtomicInteger(0);
        }
    }

    /**
     * Get the next trace object id for this object type.
     *
     * @param type the object type
     * @return the new trace object id
     */
    public static int getNextTraceId(TraceObjectType type) {
        return ID[type.ordinal()].incrementAndGet();
    }

    /**
     * The trace module used by this object.
     */
    protected Trace trace;

    public int getTraceId() {
        return trace.getTraceId();
    }

    /**
     * INTERNAL
     */
    public String getTraceObjectName() {
        return trace.getTraceObjectName();
    }

    /**
     * Check if the debug trace level is enabled.
     *
     * @return true if it is
     */
    protected boolean isDebugEnabled() {
        return trace.isDebugEnabled();
    }

    /**
     * Check if info trace level is enabled.
     *
     * @return true if it is
     */
    protected boolean isInfoEnabled() {
        return trace.isInfoEnabled();
    }

    /**
     * Write trace information as an assignment in the form
     * className prefixId = objectName.value.
     *
     * @param className the class name of the result
     * @param newType the prefix type
     * @param newId the trace object id of the created object
     * @param value the value to assign this new object to
     */
    protected void debugCodeAssign(String className, TraceObjectType newType, int newId, String value) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(className + " " + newType.getShortName() + newId + " = " + getTraceObjectName() + "."
                    + value + ";");
        }
    }

    /**
     * Write trace information as a method call in the form
     * objectName.methodName().
     *
     * @param methodName the method name
     */
    protected void debugCodeCall(String methodName) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(getTraceObjectName() + "." + methodName + "();");
        }
    }

    /**
     * Write trace information as a method call in the form
     * objectName.methodName(param) where the parameter is formatted as a long
     * value.
     *
     * @param methodName the method name
     * @param param one single long parameter
     */
    protected void debugCodeCall(String methodName, long param) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(getTraceObjectName() + "." + methodName + "(" + param + ");");
        }
    }

    /**
     * Write trace information as a method call in the form
     * objectName.methodName(param) where the parameter is formatted as a Java
     * string.
     *
     * @param methodName the method name
     * @param param one single string parameter
     */
    protected void debugCodeCall(String methodName, String param) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(getTraceObjectName() + "." + methodName + "(" + quote(param) + ");");
        }
    }

    /**
     * Write trace information in the form objectName.text.
     *
     * @param text the trace text
     */
    protected void debugCode(String text) {
        if (trace.isDebugEnabled()) {
            trace.debugCode(getTraceObjectName() + "." + text);
        }
    }

    protected void infoCode(String format, Object... args) {
        trace.infoCode(format, args);
    }

    /**
     * Log an exception and convert it to a SQL exception if required.
     *
     * @param ex the exception
     * @return the SQL exception object
     */
    protected SQLException logAndConvert(Exception ex) {
        SQLException e = DbException.toSQLException(ex);
        if (trace == null) {
            DbException.traceThrowable(e);
        } else {
            int errorCode = e.getErrorCode();
            if (errorCode >= 23000 && errorCode < 24000) {
                trace.info(e, "exception");
            } else {
                trace.error(e, "exception");
            }
        }
        return e;
    }

    /**
     * Get and throw a SQL exception meaning this feature is not supported.
     *
     * @param message the message
     * @return never returns normally
     * @throws SQLException the exception
     */
    protected SQLException unsupported(String message) throws SQLException {
        try {
            throw DbException.getUnsupportedException(message);
        } catch (Exception e) {
            return logAndConvert(e);
        }
    }

    /**
     * Format a string as a Java string literal.
     *
     * @param s the string to convert
     * @return the Java string literal
     */
    protected static String quote(String s) {
        return StringUtils.quoteJavaString(s);
    }

    /**
     * Format a time to the Java source code that represents this object.
     *
     * @param x the time to convert
     * @return the Java source code
     */
    protected static String quoteTime(java.sql.Time x) {
        if (x == null) {
            return "null";
        }
        return "Time.valueOf(\"" + x.toString() + "\")";
    }

    /**
     * Format a timestamp to the Java source code that represents this object.
     *
     * @param x the timestamp to convert
     * @return the Java source code
     */
    protected static String quoteTimestamp(java.sql.Timestamp x) {
        if (x == null) {
            return "null";
        }
        return "Timestamp.valueOf(\"" + x.toString() + "\")";
    }

    /**
     * Format a date to the Java source code that represents this object.
     *
     * @param x the date to convert
     * @return the Java source code
     */
    protected static String quoteDate(java.sql.Date x) {
        if (x == null) {
            return "null";
        }
        return "Date.valueOf(\"" + x.toString() + "\")";
    }

    /**
     * Format a big decimal to the Java source code that represents this object.
     *
     * @param x the big decimal to convert
     * @return the Java source code
     */
    protected static String quoteBigDecimal(BigDecimal x) {
        if (x == null) {
            return "null";
        }
        return "new BigDecimal(\"" + x.toString() + "\")";
    }

    /**
     * Format a byte array to the Java source code that represents this object.
     *
     * @param x the byte array to convert
     * @return the Java source code
     */
    protected static String quoteBytes(byte[] x) {
        if (x == null) {
            return "null";
        }
        return "org.lealone.util.StringUtils.convertHexToBytes(\"" + StringUtils.convertBytesToHex(x) + "\")";
    }

    /**
     * Format a string array to the Java source code that represents this
     * object.
     *
     * @param s the string array to convert
     * @return the Java source code
     */
    protected static String quoteArray(String[] s) {
        return StringUtils.quoteJavaStringArray(s);
    }

    /**
     * Format an int array to the Java source code that represents this object.
     *
     * @param s the int array to convert
     * @return the Java source code
     */
    protected static String quoteIntArray(int[] s) {
        return StringUtils.quoteJavaIntArray(s);
    }

    /**
     * Format a map to the Java source code that represents this object.
     *
     * @param map the map to convert
     * @return the Java source code
     */
    protected static String quoteMap(Map<String, Class<?>> map) {
        if (map == null) {
            return "null";
        }
        if (map.isEmpty()) {
            return "new Map()";
        }
        return "new Map() /* " + map.toString() + " */";
    }
}
