/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol;

// 协议包的类型值没有使用枚举常量的ordinal值而是自定义的值，这样允许改变枚举常量的定义顺序，
// 类型值会编码到协议包中，所以不能随便改动，不同协议包的类型值之间有意设置了间隔，用于后续加新的协议包
public enum PacketType {

    SESSION_INIT(0),
    SESSION_INIT_ACK(1),
    SESSION_CANCEL_STATEMENT(2),
    SESSION_SET_AUTO_COMMIT(3),
    SESSION_CLOSE(4),

    PREPARED_STATEMENT_PREPARE(10),
    PREPARED_STATEMENT_PREPARE_ACK(11),
    PREPARED_STATEMENT_PREPARE_READ_PARAMS(12),
    PREPARED_STATEMENT_PREPARE_READ_PARAMS_ACK(13),
    PREPARED_STATEMENT_QUERY(14),
    // PREPARED_STATEMENT_QUERY_ACK(15), // ACK直接用STATEMENT_QUERY_ACK
    PREPARED_STATEMENT_UPDATE(16),
    // PREPARED_STATEMENT_UPDATE_ACK(17),
    PREPARED_STATEMENT_GET_META_DATA(18),
    PREPARED_STATEMENT_GET_META_DATA_ACK(19),
    PREPARED_STATEMENT_CLOSE(20),

    STATEMENT_QUERY(30),
    STATEMENT_QUERY_ACK(31),
    STATEMENT_UPDATE(32),
    STATEMENT_UPDATE_ACK(33),

    BATCH_STATEMENT_UPDATE(40),
    BATCH_STATEMENT_UPDATE_ACK(41),
    BATCH_STATEMENT_PREPARED_UPDATE(42), // ACK直接用BATCH_STATEMENT_UPDATE_ACK

    RESULT_FETCH_ROWS(50),
    RESULT_FETCH_ROWS_ACK(51),
    RESULT_CHANGE_ID(52),
    RESULT_RESET(53),
    RESULT_CLOSE(54),

    LOB_READ(60),
    LOB_READ_ACK(61),

    VOID(61 + 1);

    public final int value;

    private PacketType(int value) {
        this.value = value;
    }
}
