/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import org.lealone.db.PluginBase;

public class JavaServiceExecutorFactory extends PluginBase implements ServiceExecutorFactory {

    public JavaServiceExecutorFactory() {
        super("java");
    }

    @Override
    public JavaServiceExecutor createServiceExecutor(Service service) {
        return new JavaServiceExecutor(service);
    }
}
