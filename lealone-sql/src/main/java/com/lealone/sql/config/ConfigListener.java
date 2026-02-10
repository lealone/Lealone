/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.config;

import com.lealone.common.exceptions.ConfigException;

public interface ConfigListener {

    void applyConfig(Config config) throws ConfigException;

}
