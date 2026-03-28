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

    public void init(String[] args) {
        apiKey = getApiKey(args);
        model = getModel(args);
        url = getURL(args);
    }

    @Override
    public synchronized void init(Map<String, String> config) {
        if (!isInited()) {
            apiKey = getValue(config, "LLM_API_KEY");
            model = getValue(config, "LLM_MODEL");
            url = getValue(config, "LLM_URL");
            super.init(config);
        }
    }

    public String getApiKey(String[] args) {
        return args.length == 1 ? args[0] : System.getenv("LLM_API_KEY");
    }

    public String getModel(String[] args) {
        return args.length == 2 ? args[1] : System.getenv("LLM_MODEL");
    }

    public String getURL(String[] args) {
        return args.length == 3 ? args[2] : System.getenv("LLM_URL");
    }

    public String getValue(Map<String, String> config, String key) {
        String v = MapUtils.getString(config, key, null);
        if (v == null)
            v = System.getenv(key);
        if (v == null)
            v = System.getenv(key.toLowerCase());
        if (v == null)
            v = System.getProperty(key);
        if (v == null)
            v = System.getProperty(key.toLowerCase());
        return v;
    }

    @Override
    public abstract String generateJavaCode(String userPrompt);

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return CodeAgent.class;
    }
}
