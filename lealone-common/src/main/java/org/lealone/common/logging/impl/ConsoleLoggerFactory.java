/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.logging.impl;

import org.lealone.common.logging.LoggerFactory;

public class ConsoleLoggerFactory extends LoggerFactory {
    @Override
    public ConsoleLogger createLogger(String name) {
        return new ConsoleLogger();
    }
}
