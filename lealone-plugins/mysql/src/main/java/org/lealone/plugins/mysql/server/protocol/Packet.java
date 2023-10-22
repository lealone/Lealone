/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.server.protocol;

import java.io.UnsupportedEncodingException;

import org.lealone.common.exceptions.DbException;
import org.lealone.plugins.mysql.server.util.CharsetUtil;

public abstract class Packet {

    public int packetLength;
    public byte packetId;

    /**
     * 取得数据包信息
     */
    public abstract String getPacketInfo();

    public void read(PacketInput in) {
        throw DbException.throwInternalError("read");
    }

    public void write(PacketOutput out) {
        throw DbException.throwInternalError("write");
    }

    @Override
    public String toString() {
        return new StringBuilder().append(getPacketInfo()) //
                .append("{ packetLength = ").append(packetLength) //
                .append(", packetId = ").append(packetId).append(" }").toString();
    }

    public static int getLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

    public static int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

    private static final String CODE_PAGE_1252 = "Cp1252";

    public static ResultSetHeaderPacket getHeader(int fieldCount) {
        ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
        packet.packetId = 1;
        packet.fieldCount = fieldCount;
        return packet;
    }

    private static byte[] encode(String src, String charset) {
        if (src == null) {
            return null;
        }
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    public static FieldPacket getField(String name, int type) {
        FieldPacket packet = new FieldPacket();
        packet.charsetIndex = CharsetUtil.getIndex(CODE_PAGE_1252);
        packet.name = encode(name, CODE_PAGE_1252);
        packet.type = (byte) type;
        return packet;
    }

    // public static FieldPacket getField(String name, String orgName, int type) {
    // FieldPacket packet = new FieldPacket();
    // packet.charsetIndex = CharsetUtil.getIndex(CODE_PAGE_1252);
    // packet.name = encode(name, CODE_PAGE_1252);
    // packet.orgName = encode(orgName, CODE_PAGE_1252);
    // packet.type = (byte) type;
    // return packet;
    // }

    // public static FieldPacket getField(PacketInput in, String fieldName) {
    // FieldPacket field = new FieldPacket();
    // field.read(in);
    // field.name = encode(fieldName, CODE_PAGE_1252);
    // field.packetLength = field.calcPacketSize();
    // return field;
    // }

    // public static ErrorPacket getShutdown() {
    // ErrorPacket error = new ErrorPacket();
    // error.packetId = 1;
    // error.errno = ErrorCode.ER_SERVER_SHUTDOWN;
    // error.message = "The server has been shutdown".getBytes();
    // return error;
    // }
}
