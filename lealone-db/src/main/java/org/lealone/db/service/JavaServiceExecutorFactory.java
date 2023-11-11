/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

public class JavaServiceExecutorFactory extends ServiceExecutorFactoryBase {

    public JavaServiceExecutorFactory() {
        super("java");
    }

    @Override
    public JavaServiceExecutor createServiceExecutor(Service service) {
        return new JavaServiceExecutor(service);
    }
}
