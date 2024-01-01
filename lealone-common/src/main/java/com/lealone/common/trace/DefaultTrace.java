/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.common.trace;

import java.text.MessageFormat;

import com.lealone.common.util.StringUtils;

/**
 * This class represents a default trace module.
 * 
 * @author H2 Group
 * @author zhh
 */
class DefaultTrace implements Trace {

    private final TraceWriter traceWriter;
    private String module;
    private final String traceObjectName;
    private final String lineSeparator;
    private final int id;
    private int traceLevel = TraceSystem.PARENT;

    DefaultTrace(TraceWriter traceWriter, String module) {
        this(traceWriter, module, null, -1);
    }

    DefaultTrace(TraceWriter traceWriter, String module, String traceObjectName, int id) {
        this.traceWriter = traceWriter;
        this.module = module;
        this.traceObjectName = traceObjectName;
        this.id = id;
        this.lineSeparator = System.lineSeparator();
    }

    @Override
    public int getTraceId() {
        return id;
    }

    @Override
    public String getTraceObjectName() {
        return traceObjectName;
    }

    @Override
    public DefaultTrace setType(TraceModuleType type) {
        module = type.name().toLowerCase();
        return this;
    }

    /**
     * Set the trace level of this component.
     * This setting overrides the parent trace level.
     *
     * @param level the new level
     */
    public void setLevel(int level) {
        this.traceLevel = level;
    }

    private boolean isEnabled(int level) {
        if (this.traceLevel == TraceSystem.PARENT) {
            return traceWriter.isEnabled(level);
        }
        return level <= this.traceLevel;
    }

    @Override
    public boolean isInfoEnabled() {
        return isEnabled(TraceSystem.INFO);
    }

    @Override
    public boolean isDebugEnabled() {
        return isEnabled(TraceSystem.DEBUG);
    }

    @Override
    public void error(Throwable t, String s) {
        if (isEnabled(TraceSystem.ERROR)) {
            traceWriter.write(TraceSystem.ERROR, module, s, t);
        }
    }

    @Override
    public void error(Throwable t, String s, Object... params) {
        if (isEnabled(TraceSystem.ERROR)) {
            s = MessageFormat.format(s, params);
            traceWriter.write(TraceSystem.ERROR, module, s, t);
        }
    }

    @Override
    public void info(String s) {
        if (isEnabled(TraceSystem.INFO)) {
            traceWriter.write(TraceSystem.INFO, module, s, null);
        }
    }

    @Override
    public void info(String s, Object... params) {
        if (isEnabled(TraceSystem.INFO)) {
            s = MessageFormat.format(s, params);
            traceWriter.write(TraceSystem.INFO, module, s, null);
        }
    }

    @Override
    public void info(Throwable t, String s) {
        if (isEnabled(TraceSystem.INFO)) {
            traceWriter.write(TraceSystem.INFO, module, s, t);
        }
    }

    @Override
    public void infoSQL(String sql, String params, int count, long time) {
        if (!isEnabled(TraceSystem.INFO)) {
            return;
        }
        StringBuilder buff = new StringBuilder(sql.length() + params.length() + 20);
        buff.append(lineSeparator).append("/*SQL");
        boolean space = false;
        if (params.length() > 0) {
            // This looks like a bug, but it is intentional:
            // If there are no parameters, the SQL statement is
            // the rest of the line. If there are parameters, they
            // are appended at the end of the line. Knowing the size
            // of the statement simplifies separating the SQL statement
            // from the parameters (no need to parse).
            space = true;
            buff.append(" l:").append(sql.length());
        }
        if (count > 0) {
            space = true;
            buff.append(" #:").append(count);
        }
        if (time > 0) {
            space = true;
            buff.append(" t:").append(time);
        }
        if (!space) {
            buff.append(' ');
        }
        buff.append("*/").append(StringUtils.javaEncode(sql)).append(StringUtils.javaEncode(params))
                .append(';');
        sql = buff.toString();
        traceWriter.write(TraceSystem.INFO, module, sql, null);
    }

    @Override
    public void infoCode(String format, Object... args) {
        if (isEnabled(TraceSystem.INFO)) {
            String code = String.format(format, args);
            traceWriter.write(TraceSystem.INFO, module, lineSeparator + "/**/" + code, null);
        }
    }

    @Override
    public void debug(String s) {
        if (isEnabled(TraceSystem.DEBUG)) {
            traceWriter.write(TraceSystem.DEBUG, module, s, null);
        }
    }

    @Override
    public void debug(String s, Object... params) {
        if (isEnabled(TraceSystem.DEBUG)) {
            s = MessageFormat.format(s, params);
            traceWriter.write(TraceSystem.DEBUG, module, s, null);
        }
    }

    @Override
    public void debug(Throwable t, String s) {
        if (isEnabled(TraceSystem.DEBUG)) {
            traceWriter.write(TraceSystem.DEBUG, module, s, t);
        }
    }

    @Override
    public void debugCode(String java) {
        if (isEnabled(TraceSystem.DEBUG)) {
            traceWriter.write(TraceSystem.DEBUG, module, lineSeparator + "/**/" + java, null);
        }
    }
}
