/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.trace;

class NoTrace implements Trace {

    NoTrace() {
    }

    @Override
    public int getTraceId() {
        return -1;
    }

    @Override
    public String getTraceObjectName() {
        return "";
    }

    @Override
    public NoTrace setType(TraceModuleType type) {
        return this;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void error(Throwable t, String s) {
    }

    @Override
    public void error(Throwable t, String s, Object... params) {
    }

    @Override
    public void info(String s) {
    }

    @Override
    public void info(String s, Object... params) {
    }

    @Override
    public void info(Throwable t, String s) {
    }

    @Override
    public void infoSQL(String sql, String params, int count, long time) {
    }

    @Override
    public void infoCode(String format, Object... args) {
    }

    @Override
    public void debug(String s, Object... params) {
    }

    @Override
    public void debug(String s) {
    }

    @Override
    public void debug(Throwable t, String s) {
    }

    @Override
    public void debugCode(String java) {
    }
}
