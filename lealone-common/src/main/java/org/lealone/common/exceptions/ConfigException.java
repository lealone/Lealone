/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.exceptions;

public class ConfigException extends RuntimeException {

    public ConfigException(String msg) {
        super(msg);
    }

    public ConfigException(String msg, Throwable e) {
        super(msg, e);
    }
}
