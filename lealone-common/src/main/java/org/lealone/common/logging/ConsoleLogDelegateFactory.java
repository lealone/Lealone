/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.logging;

import org.lealone.common.logging.spi.LogDelegate;
import org.lealone.common.logging.spi.LogDelegateFactory;

public class ConsoleLogDelegateFactory implements LogDelegateFactory {

    @Override
    public LogDelegate createDelegate(String name) {
        return new ConsoleLogDelegate();
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

}
