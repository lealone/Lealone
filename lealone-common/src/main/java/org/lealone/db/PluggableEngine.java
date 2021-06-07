/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.util.Map;

public interface PluggableEngine {

    String getName();

    void init(Map<String, String> config);

    void close();

}
