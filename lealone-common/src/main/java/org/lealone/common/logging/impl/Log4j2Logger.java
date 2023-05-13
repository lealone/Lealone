/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.logging.impl;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.message.FormattedMessage;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;

class Log4j2Logger implements Logger {

    private final static String FQCN = Logger.class.getCanonicalName();
    private final ExtendedLogger logger;

    Log4j2Logger(String name) {
        logger = (ExtendedLogger) org.apache.logging.log4j.LogManager.getLogger(name);
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void fatal(Object message) {
        log(Level.FATAL, message);
    }

    @Override
    public void fatal(Object message, Throwable t) {
        log(Level.FATAL, message, t);
    }

    @Override
    public void error(Object message) {
        log(Level.ERROR, message);
    }

    @Override
    public void error(Object message, Object... params) {
        log(Level.ERROR, message.toString(), params);
    }

    @Override
    public void error(Object message, Throwable t) {
        log(Level.ERROR, message, t);
    }

    @Override
    public void error(Object message, Throwable t, Object... params) {
        log(Level.ERROR, message.toString(), t, params);
    }

    @Override
    public void warn(Object message) {
        log(Level.WARN, message);
    }

    @Override
    public void warn(Object message, Object... params) {
        log(Level.WARN, message.toString(), params);
    }

    @Override
    public void warn(Object message, Throwable t) {
        log(Level.WARN, message, t);
    }

    @Override
    public void warn(Object message, Throwable t, Object... params) {
        log(Level.WARN, message.toString(), t, params);
    }

    @Override
    public void info(Object message) {
        log(Level.INFO, message);
    }

    @Override
    public void info(Object message, Object... params) {
        log(Level.INFO, message.toString(), params);
    }

    @Override
    public void info(Object message, Throwable t) {
        log(Level.INFO, message, t);
    }

    @Override
    public void info(Object message, Throwable t, Object... params) {
        log(Level.INFO, message.toString(), t, params);
    }

    @Override
    public void debug(Object message) {
        log(Level.DEBUG, message);
    }

    @Override
    public void debug(Object message, Object... params) {
        log(Level.DEBUG, message.toString(), params);
    }

    @Override
    public void debug(Object message, Throwable t) {
        log(Level.DEBUG, message, t);
    }

    @Override
    public void debug(Object message, Throwable t, Object... params) {
        log(Level.DEBUG, message.toString(), t, params);
    }

    @Override
    public void trace(Object message) {
        log(Level.TRACE, message);
    }

    @Override
    public void trace(Object message, Object... params) {
        log(Level.TRACE, message.toString(), params);
    }

    @Override
    public void trace(Object message, Throwable t) {
        log(Level.TRACE, message.toString(), t);
    }

    @Override
    public void trace(Object message, Throwable t, Object... params) {
        log(Level.TRACE, message.toString(), t, params);
    }

    private void log(Level level, Object message) {
        log(level, message, null);
    }

    private void log(Level level, Object message, Throwable t) {
        t = DbException.getCause(t);
        if (message instanceof Message) {
            logger.logIfEnabled(FQCN, level, null, (Message) message, t);
        } else {
            logger.logIfEnabled(FQCN, level, null, message, t);
        }
    }

    private void log(Level level, String message, Object... params) {
        logger.logIfEnabled(FQCN, level, null, message, params);
    }

    private void log(Level level, String message, Throwable t, Object... params) {
        t = DbException.getCause(t);
        logger.logIfEnabled(FQCN, level, null, new FormattedMessage(message, params), t);
    }
}
