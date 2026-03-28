/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.agent;

import com.lealone.db.plugin.Plugin;

public interface CodeAgent extends Plugin {

    public String generateJavaCode(String userPrompt);

}
