/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.logging;

import java.util.concurrent.ConcurrentHashMap;

import com.lealone.common.logging.impl.ConsoleLoggerFactory;
import com.lealone.common.logging.impl.Log4j2LoggerFactory;
import com.lealone.common.util.Utils;

public abstract class LoggerFactory {

    protected abstract Logger createLogger(String name);

    public static final String LOGGER_FACTORY_CLASS_NAME = "lealone.logger.factory";
    private static final ConcurrentHashMap<String, Logger> loggers = new ConcurrentHashMap<>();
    private static final LoggerFactory loggerFactory = getLoggerFactory();

    // 优先使用自定义的LoggerFactory，然后是log4j2，最后是Console
    private static LoggerFactory getLoggerFactory() {
        String factoryClassName = null;
        try {
            factoryClassName = System.getProperty(LOGGER_FACTORY_CLASS_NAME);
        } catch (Exception e) {
        }
        if (factoryClassName != null) {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try {
                Class<?> clz = loader.loadClass(factoryClassName);
                return Utils.newInstance(clz);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Error instantiating class \"" + factoryClassName + "\"", e);
            }
        } else if (LoggerFactory.class
                .getResource("/org/apache/logging/log4j/spi/ExtendedLogger.class") != null) {
            return new Log4j2LoggerFactory();
        } else {
            return new ConsoleLoggerFactory();
        }
    }

    public static Logger getLogger(Class<?> clazz) {
        String name = clazz.isAnonymousClass() ? clazz.getEnclosingClass().getCanonicalName()
                : clazz.getCanonicalName();
        return getLogger(name);
    }

    public static Logger getLogger(String name) {
        Logger logger = loggers.get(name);
        if (logger == null) {
            logger = loggerFactory.createLogger(name);
            Logger oldLogger = loggers.putIfAbsent(name, logger);
            if (oldLogger != null) {
                logger = oldLogger;
            }
        }
        return logger;
    }

    public static void removeLogger(String name) {
        loggers.remove(name);
    }
}
