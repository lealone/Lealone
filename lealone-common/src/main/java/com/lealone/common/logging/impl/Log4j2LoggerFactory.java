/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.logging.impl;

import com.lealone.common.logging.LoggerFactory;

public class Log4j2LoggerFactory extends LoggerFactory {
    @Override
    public Log4j2Logger createLogger(String name) {
        return new Log4j2Logger(name);
    }
}
