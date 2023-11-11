/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.util.Map;

import org.lealone.common.util.MapUtils;

public abstract class PluginBase implements Plugin {

    protected String name;
    protected Map<String, String> config;

    public PluginBase(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Map<String, String> getConfig() {
        return config;
    }

    @Override
    public void init(Map<String, String> config) {
        this.config = config;
        String pluginName = MapUtils.getString(config, "plugin_name", null);
        if (pluginName != null)
            setName(pluginName); // 使用create plugin创建插件对象时用命令指定的名称覆盖默认值

        Class<Plugin> pluginClass = getPluginClass0();
        Plugin p = PluginManager.getPlugin(pluginClass, getName());
        if (p == null) {
            PluginManager.register(pluginClass, this);
        }
    }

    @Override
    public void close() {
        Class<Plugin> pluginClass = getPluginClass0();
        Plugin p = PluginManager.getPlugin(pluginClass, getName());
        if (p != null)
            PluginManager.deregister(pluginClass, p);
    }

    @SuppressWarnings("unchecked")
    private Class<Plugin> getPluginClass0() {
        return (Class<Plugin>) getPluginClass();
    }

    public abstract Class<? extends Plugin> getPluginClass();
}
