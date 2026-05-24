/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent;

import java.util.Map;

import com.lealone.db.DataHandler;
import com.lealone.db.Database;
import com.lealone.db.session.ServerSession;
import com.lealone.db.session.Session;

// 生成应用代码的CodeAgent
public abstract class AppCodeAgent extends CodeAgentBase {

    public AppCodeAgent(String name) {
        super(name);
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        afterInit();
    }

    protected void afterInit() {
    }

    private static final String promptPrefix = "请根据需求生成java代码实现类，不要生成接口，代码采用驼峰格式，" + //
            "只输出纯代码，不要任何解释，不要```java标记，不要多余文字：";

    @Override
    public String getPromptPrefix() {
        return promptPrefix;
    }

    @Override
    public String execute(String userPrompt, DataHandler db, Session session) {
        return new AgentExecutor(userPrompt, (Database) db, (ServerSession) session).execute();
    }
}
