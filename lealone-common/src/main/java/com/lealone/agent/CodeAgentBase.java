/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent;

import java.util.Map;

import com.lealone.common.util.MapUtils;
import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginBase;

public abstract class CodeAgentBase extends PluginBase implements CodeAgent {

    protected String apiKey;
    protected String model;
    protected String url;

    public CodeAgentBase(String name) {
        super(name);
    }

    @Override
    public synchronized void init(Map<String, String> config) {
        if (!isInited()) {
            apiKey = getValue(config, "API_KEY");
            model = getValue(config, "MODEL");
            url = getValue(config, "URL");
            super.init(config);
        }
    }

    public String getValue(Map<String, String> config, String key) {
        String v = MapUtils.getString(config, key, null);
        if (v == null) {
            key = "LLM_" + key;
            v = System.getenv(key);
        }
        if (v == null)
            v = System.getenv(key.toLowerCase());
        if (v == null)
            v = System.getProperty(key);
        if (v == null)
            v = System.getProperty(key.toLowerCase());
        return v;
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return CodeAgent.class;
    }
}
