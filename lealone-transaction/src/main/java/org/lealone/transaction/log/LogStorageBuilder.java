/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction.log;

import java.util.HashMap;

import org.lealone.common.util.DataUtils;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;

/**
 * A builder for an LogStorage.
 * 
 * @author H2 Group
 * @author zhh
 */
public class LogStorageBuilder extends StorageBuilder {

    /**
     * Open the storage.
     * 
     * @return the opened storage
     */

    /**
     * Read the configuration from a string.
     * 
     * @param s the string representation
     * @return the builder
     */
    public static LogStorageBuilder fromString(String s) {
        HashMap<String, String> config = DataUtils.parseMap(s);
        LogStorageBuilder builder = new LogStorageBuilder();
        builder.config.putAll(config);
        return builder;
    }

    @Override
    public Storage openStorage() {
        return null;
    }

    public LogStorage open() {
        return new LogStorage(config);
    }
}
