/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.config;

import org.lealone.common.exceptions.ConfigException;

public interface ConfigLoader {

    /**
     * Loads a {@link Config} object to use to configure a node.
     *
     * @return the {@link Config} to use.
     * @throws ConfigException if the configuration cannot be properly loaded.
     */
    Config loadConfig() throws ConfigException;

    default Config loadConfig(boolean lazyApply) throws ConfigException {
        return loadConfig();
    }
}
