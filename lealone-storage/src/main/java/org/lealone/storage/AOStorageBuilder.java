/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage;

import java.util.HashMap;
import java.util.Map;

import org.lealone.common.util.DataUtils;

/**
 * A builder for AOStorage.
 * 
 * @author H2 Group
 * @author zhh
 */
public class AOStorageBuilder extends StorageBuilder {

    public AOStorageBuilder() {
    }

    public AOStorageBuilder(Map<String, String> defaultConfig) {
        if (defaultConfig != null)
            config.putAll(defaultConfig);
    }

    /**
     * Open the storage.
     * 
     * @return the opened storage
     */
    @Override
    public AOStorage openStorage() {
        return new AOStorage(config);
    }

    /**
     * Read the configuration from a string.
     * 
     * @param s the string representation
     * @return the builder
     */
    public static AOStorageBuilder fromString(String s) {
        HashMap<String, String> config = DataUtils.parseMap(s);
        AOStorageBuilder builder = new AOStorageBuilder();
        builder.config.putAll(config);
        return builder;
    }

}
