/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import org.lealone.db.Plugin;

public interface ServiceExecutorFactory extends Plugin {

    ServiceExecutor createServiceExecutor(Service service);

    default boolean supportsGenCode() {
        return false;
    }

    default void genCode(Service service) {
    }
}
