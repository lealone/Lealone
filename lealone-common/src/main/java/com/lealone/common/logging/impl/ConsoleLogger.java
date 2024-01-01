/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.logging.impl;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;

class ConsoleLogger implements Logger {

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public boolean isInfoEnabled() {
        return true;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public void fatal(Object message) {
        log(message);
    }

    @Override
    public void fatal(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void error(Object message) {
        log(message);
    }

    @Override
    public void error(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void error(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void error(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

    @Override
    public void warn(Object message) {
        log(message);
    }

    @Override
    public void warn(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void warn(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void warn(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

    @Override
    public void info(Object message) {
        log(message);
    }

    @Override
    public void info(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void info(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void info(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

    @Override
    public void debug(Object message) {
        log(message);
    }

    @Override
    public void debug(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void debug(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void debug(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

    @Override
    public void trace(Object message) {
        log(message);
    }

    @Override
    public void trace(Object message, Object... params) {
        log(message, params);
    }

    @Override
    public void trace(Object message, Throwable t) {
        log(message, t);
    }

    @Override
    public void trace(Object message, Throwable t, Object... params) {
        log(message, t, params);
    }

    private void log(Object message) {
        System.out.println(message);
    }

    private void log(Object message, Object... params) {
        char[] chars = message.toString().toCharArray();
        int length = chars.length;
        StringBuilder s = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            if (chars[i] == '{' && chars[i + 1] == '}') {
                s.append("%s");
                i++;
            } else {
                s.append(chars[i]);
            }
        }
        System.out.println(String.format(s.toString(), params));
    }

    private void log(Object message, Throwable t) {
        log(message);
        if (t != null)
            DbException.getCause(t).printStackTrace(System.err);
    }

    private void log(Object message, Throwable t, Object... params) {
        log(message, params);
        if (t != null)
            DbException.getCause(t).printStackTrace(System.err);
    }
}
