/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.plugin;

import java.util.Map;

public interface Plugin {

    String getName();

    Map<String, String> getConfig();

    void init(Map<String, String> config);

    void close();

    void start();

    void stop();

    boolean isInited();

    boolean isStarted();

    boolean isStopped();

    State getState();

    enum State {
        NONE,
        INITED,
        STARTED,
        STOPPED;
    }
}
