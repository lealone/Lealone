/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import java.util.Map;

import org.lealone.db.scheduler.SchedulerFactory;

public interface PageOperationHandlerFactory {

    PageOperationHandler getPageOperationHandler();

    public static PageOperationHandlerFactory create(Map<String, String> config) {
        return SchedulerFactory.create(config);
    }
}
