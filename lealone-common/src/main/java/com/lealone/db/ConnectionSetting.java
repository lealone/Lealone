/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db;

public enum ConnectionSetting {
    IGNORE_UNKNOWN_SETTINGS,
    USER,
    PASSWORD,
    PASSWORD_HASH,
    IS_SERVICE_CONNECTION,
    NET_FACTORY_NAME,
    NETWORK_TIMEOUT,
    TRACE_ENABLED,

    IS_SHARED,
    MAX_SHARED_SIZE,
    SOCKET_RECV_BUFFER_SIZE,
    SOCKET_SEND_BUFFER_SIZE,
    MAX_PACKET_SIZE,

    SESSION_FACTORY_NAME,
    AUTO_RECONNECT,
    SCHEDULER_COUNT,
}
