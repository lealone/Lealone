/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.util.Map;

public class PluginBase implements Plugin {

    protected final String name;
    protected Map<String, String> config;

    public PluginBase(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<String, String> getConfig() {
        return config;
    }

    @Override
    public void init(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public void close() {
    }
}
