/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.logging.impl;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.DefaultConfiguration;

import com.lealone.common.logging.LoggerFactory;

public class Log4j2LoggerFactory extends LoggerFactory {

    public Log4j2LoggerFactory() {
        // 如果找不到Log4j2的配置文件，默认级别设为INFO
        System.setProperty(DefaultConfiguration.DEFAULT_LEVEL, Level.INFO.name());
    }

    @Override
    public Log4j2Logger createLogger(String name) {
        return new Log4j2Logger(name);
    }
}
