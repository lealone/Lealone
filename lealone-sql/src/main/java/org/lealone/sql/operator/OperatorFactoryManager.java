/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.operator;

import org.lealone.db.PluggableEngineManager;

public class OperatorFactoryManager extends PluggableEngineManager<OperatorFactory> {

    private static final OperatorFactoryManager instance = new OperatorFactoryManager();

    public static OperatorFactoryManager getInstance() {
        return instance;
    }

    private OperatorFactoryManager() {
        super(OperatorFactory.class);
    }

    public static OperatorFactory getFactory(String name) {
        return instance.getEngine(name);
    }

    public static void registerFactory(OperatorFactory factory) {
        instance.registerEngine(factory);
    }

    public static void deregisterFactory(OperatorFactory factory) {
        instance.deregisterEngine(factory);
    }
}
