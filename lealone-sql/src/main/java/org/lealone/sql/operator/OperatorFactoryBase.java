/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.operator;

import java.util.Map;

public abstract class OperatorFactoryBase implements OperatorFactory {

    protected final String name;
    protected Map<String, String> config;

    public OperatorFactoryBase(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void init(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public void close() {
    }
}
