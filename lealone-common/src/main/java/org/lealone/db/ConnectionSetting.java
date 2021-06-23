/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

public enum ConnectionSetting {
    IGNORE_UNKNOWN_SETTINGS,
    INIT,
    USER,
    PASSWORD,
    PASSWORD_HASH,
    IS_ROOT,
    IS_SERVICE_CONNECTION,
    NET_FACTORY_NAME,
    NETWORK_TIMEOUT,
    TRACE_ENABLED;
}
