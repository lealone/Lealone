/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.service;

import com.lealone.db.plugin.Plugin;

public interface ServiceExecutorFactory extends Plugin {

    ServiceExecutor createServiceExecutor(Service service);

    default boolean supportsGenCode() {
        return false;
    }

    default void genCode(Service service) {
    }
}
