/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.logging.impl;

import com.lealone.common.logging.LoggerFactory;

public class ConsoleLoggerFactory extends LoggerFactory {
    @Override
    public ConsoleLogger createLogger(String name) {
        return new ConsoleLogger();
    }
}
