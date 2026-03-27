/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent.llm;

public abstract class BaseLLM {

    protected String apiKey;
    protected String model;
    protected String url;

    public void init(String[] args) {
        apiKey = getApiKey(args);
        model = getModel(args);
        url = getURL(args);
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

    public abstract String generateJavaCode(String userPrompt);
}
