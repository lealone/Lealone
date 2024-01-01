/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.server.protocol;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.lealone.common.exceptions.DbException;
import com.lealone.net.NetBuffer;
import com.lealone.plugins.mysql.server.MySQLServerConnection;

public class PacketInput {

    private static final long NULL_LENGTH = -1;
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final MySQLServerConnection conn;
    private final int packetLength;
    private final byte packetId;
    private final ByteBuffer buffer;

    public PacketInput(MySQLServerConnection conn, byte packetId, NetBuffer netBuffer) {
        this.conn = conn;
        this.packetLength = netBuffer.length() + 4;
        this.packetId = packetId;
        this.buffer = netBuffer.getByteBuffer();
    }

    public int getPacketLength() {
        return packetLength;
    }

    public byte getPacketId() {
        return packetId;
    }

    public int position() {
        return buffer.position();
    }

    public void position(int i) {
        buffer.position(i);
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    public byte[] readBytes(int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0, bytes.length);
        return bytes;
    }

    public byte read() {
        return buffer.get();
    }

    public int readUB2() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        return i;
    }

    public int readUB3() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        return i;
    }

    public long readUB4() {
        long l = read() & 0xff;
        l |= (long) (read() & 0xff) << 8;
        l |= (long) (read() & 0xff) << 16;
        l |= (long) (read() & 0xff) << 24;
        return l;
    }

    public int readInt() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        i |= (read() & 0xff) << 24;
        return i;
    }

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public long readLong() {
        long l = read() & 0xff;
        l |= (long) (read() & 0xff) << 8;
        l |= (long) (read() & 0xff) << 16;
        l |= (long) (read() & 0xff) << 24;
        l |= (long) (read() & 0xff) << 32;
        l |= (long) (read() & 0xff) << 40;
        l |= (long) (read() & 0xff) << 48;
        l |= (long) (read() & 0xff) << 56;
        return l;
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public long readLength() {
        int length = read() & 0xff;
        switch (length) {
        case 251:
            return NULL_LENGTH;
        case 252:
            return readUB2();
        case 253:
            return readUB3();
        case 254:
            return readLong();
        default:
            return length;
        }
    }

    public byte[] readBytesWithLength() {
        int length = (int) readLength();
        if (length <= 0) {
            return EMPTY_BYTES;
        }
        return readBytes(length);
    }

    public String readString(String charset) {
        if (!hasRemaining()) {
            return null;
        }
        return newString(readBytes(buffer.remaining()), charset);
    }

    public String readStringWithNull() {
        if (!hasRemaining()) {
            return null;
        }
        int offset = -1;
        int position = buffer.position();
        int length = buffer.remaining();
        int size = position + length;
        for (int i = position; i < size; i++) {
            if (buffer.get(i) == 0) {
                offset = i;
                break;
            }
        }
        if (offset == -1) {
            return new String(readBytes(length));
        }
        if (offset > position) {
            length = offset - position;
            String str = new String(readBytes(length));
            read();
            return str;
        } else {
            read();
            return null;
        }
    }

    public String readStringWithLength() {
        int length = (int) readLength();
        if (length <= 0) {
            return null;
        }
        return new String(readBytes(length));
    }

    public String readStringWithLength(String charset) {
        int length = (int) readLength();
        if (length <= 0) {
            return null;
        }
        return newString(readBytes(length), charset);
    }

    public Time readTime() {
        position(position() + 2);
        int hour = read();
        int minute = read();
        int second = read();
        Calendar cal = conn.getCalendar();
        cal.set(0, 0, 0, hour, minute, second);
        return new Time(cal.getTimeInMillis());
    }

    public java.util.Date readDate() {
        byte length = read();
        int year = readUB2();
        byte month = read();
        byte date = read();
        int hour = read();
        int minute = read();
        int second = read();
        if (length == 11) {
            long nanos = readUB4();
            Calendar cal = conn.getCalendar();
            cal.set(year, --month, date, hour, minute, second);
            Timestamp time = new Timestamp(cal.getTimeInMillis());
            time.setNanos((int) nanos);
            return time;
        } else {
            Calendar cal = conn.getCalendar();
            cal.set(year, --month, date, hour, minute, second);
            return new java.sql.Date(cal.getTimeInMillis());
        }
    }

    public BigDecimal readBigDecimal() {
        String src = readStringWithLength();
        return src == null ? null : new BigDecimal(src);
    }

    private static String newString(byte[] bytes, String charsetName) {
        try {
            return new String(bytes, charsetName);
        } catch (UnsupportedEncodingException e) {
            throw DbException.convert(e);
        }
    }
}
