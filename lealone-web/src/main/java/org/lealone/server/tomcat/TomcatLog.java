/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.tomcat;

import org.apache.juli.logging.Log;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;

public class TomcatLog implements Log {

    private final Logger logger;

    public TomcatLog() {
        logger = null;
    }

    public TomcatLog(String name) {
        logger = LoggerFactory.getLogger(name);
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
        logger.fatal(message);
    }

    @Override
    public void fatal(Object message, Throwable t) {
        logger.fatal(message, t);
    }

    @Override
    public void error(Object message) {
        logger.error(message);
    }

    public void error(Object message, Object... params) {
        logger.error(message, params);
    }

    @Override
    public void error(Object message, Throwable t) {
        logger.error(message, t);
    }

    public void error(Object message, Throwable t, Object... params) {
        logger.error(message, t, params);
    }

    @Override
    public void warn(Object message) {
        logger.warn(message);
    }

    public void warn(Object message, Object... params) {
        logger.warn(message, params);
    }

    @Override
    public void warn(Object message, Throwable t) {
        logger.warn(message, t);
    }

    public void warn(Object message, Throwable t, Object... params) {
        logger.warn(message, t, params);
    }

    @Override
    public void info(Object message) {
        logger.info(message);
    }

    public void info(Object message, Object... params) {
        logger.info(message, params);
    }

    @Override
    public void info(Object message, Throwable t) {
        logger.info(message, t);
    }

    public void info(Object message, Throwable t, Object... params) {
        logger.info(message, t, params);
    }

    @Override
    public void debug(Object message) {
        logger.debug(message);
    }

    public void debug(Object message, Object... params) {
        logger.debug(message, params);
    }

    @Override
    public void debug(Object message, Throwable t) {
        logger.debug(message, t);
    }

    public void debug(Object message, Throwable t, Object... params) {
        logger.debug(message, t, params);
    }

    @Override
    public void trace(Object message) {
        logger.trace(message);
    }

    public void trace(Object message, Object... params) {
        logger.trace(message, params);
    }

    @Override
    public void trace(Object message, Throwable t) {
        logger.trace(message, t);
    }

    public void trace(Object message, Throwable t, Object... params) {
        logger.trace(message, t, params);
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }

    @Override
    public boolean isFatalEnabled() {
        return true;
    }
}
