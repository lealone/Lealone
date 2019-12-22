/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.net;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.Session;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.SimpleResultSet;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueByte;
import org.lealone.db.value.ValueBytes;
import org.lealone.db.value.ValueDate;
import org.lealone.db.value.ValueDecimal;
import org.lealone.db.value.ValueDouble;
import org.lealone.db.value.ValueFloat;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueJavaObject;
import org.lealone.db.value.ValueLob;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueResultSet;
import org.lealone.db.value.ValueShort;
import org.lealone.db.value.ValueString;
import org.lealone.db.value.ValueStringFixed;
import org.lealone.db.value.ValueStringIgnoreCase;
import org.lealone.db.value.ValueTime;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.db.value.ValueUuid;
import org.lealone.storage.PageKey;

/**
 * The transfer class is used to receive Value objects.
 * It is used on both the client side, and on the server side.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TransferInputStream implements NetInputStream {

    private final DataInputStream in;
    private Session session;

    public TransferInputStream(NetBuffer inBuffer) {
        in = new DataInputStream(new NetBufferInputStream(inBuffer));
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public DataInputStream getDataInputStream() {
        return in;
    }

    public void closeInputStream() {
        if (in != null)
            try {
                in.close();
            } catch (IOException e) {
                // 最终只是回收NetBuffer，不应该发生异常
                throw DbException.throwInternalError();
            }
    }

    /**
     * Read a boolean.
     *
     * @return the value
     */
    @Override
    public boolean readBoolean() throws IOException {
        return in.readByte() == 1;
    }

    /**
     * Read a byte.
     *
     * @return the value
     */
    public byte readByte() throws IOException {
        return in.readByte();
    }

    /**
     * Read an int.
     *
     * @return the value
     */
    @Override
    public int readInt() throws IOException {
        return in.readInt();
    }

    /**
     * Read a long.
     *
     * @return the value
     */
    @Override
    public long readLong() throws IOException {
        return in.readLong();
    }

    /**
     * Read a double.
     *
     * @return the value
     */
    private double readDouble() throws IOException {
        return in.readDouble();
    }

    /**
     * Read a float.
     *
     * @return the value
     */
    private float readFloat() throws IOException {
        return in.readFloat();
    }

    /**
     * Read a string.
     *
     * @return the value
     */
    @Override
    public String readString() throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        StringBuilder buff = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            buff.append(in.readChar());
        }
        String s = buff.toString();
        s = StringUtils.cache(s);
        return s;
    }

    /**
     * Read a byte buffer.
     *
     * @return the value
     */
    @Override
    public ByteBuffer readByteBuffer() throws IOException {
        byte[] b = readBytes();
        if (b == null)
            return null;
        else
            return ByteBuffer.wrap(b);
    }

    /**
     * Read a byte array.
     *
     * @return the value
     */
    @Override
    public byte[] readBytes() throws IOException {
        int len = readInt();
        if (len == -1) {
            return null;
        }
        byte[] b = DataUtils.newBytes(len);
        in.readFully(b);
        return b;
    }

    /**
     * Read a number of bytes.
     *
     * @param buff the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    @Override
    public void readBytes(byte[] buff, int off, int len) throws IOException {
        in.readFully(buff, off, len);
    }

    @Override
    public PageKey readPageKey() throws IOException {
        Object value = readValue();
        boolean first = readBoolean();
        return new PageKey(value, first);
    }

    /**
    * Read a value.
    *
    * @return the value
    */
    @Override
    public Value readValue() throws IOException {
        int type = readInt();
        switch (type) {
        case Value.NULL:
            return ValueNull.INSTANCE;
        case Value.BYTES:
            return ValueBytes.getNoCopy(readBytes());
        case Value.JAVA_OBJECT:
            return ValueJavaObject.getNoCopy(null, readBytes());
        case Value.UUID:
            return ValueUuid.get(readLong(), readLong());
        case Value.BOOLEAN:
            return ValueBoolean.get(readBoolean());
        case Value.BYTE:
            return ValueByte.get(readByte());
        case Value.DATE:
            return ValueDate.fromDateValue(readLong());
        case Value.TIME:
            return ValueTime.fromNanos(readLong());
        case Value.TIMESTAMP:
            return ValueTimestamp.fromDateValueAndNanos(readLong(), readLong());
        case Value.DECIMAL:
            return ValueDecimal.get(new BigDecimal(readString()));
        case Value.DOUBLE:
            return ValueDouble.get(readDouble());
        case Value.FLOAT:
            return ValueFloat.get(readFloat());
        case Value.SHORT:
            return ValueShort.get((short) readInt());
        case Value.INT:
            return ValueInt.get(readInt());
        case Value.LONG:
            return ValueLong.get(readLong());
        case Value.STRING:
            return ValueString.get(readString());
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString());
        case Value.STRING_FIXED:
            return ValueStringFixed.get(readString());
        case Value.BLOB:
        case Value.CLOB: {
            long length = readLong();
            if (length == -1) {
                int tableId = readInt();
                long id = readLong();
                byte[] hmac = readBytes();
                long precision = readLong();
                return ValueLob.create(type, session.getDataHandler(), tableId, id, hmac, precision);
            }
            byte[] small;
            int len = (int) length;
            if (type == Value.BLOB) {
                small = new byte[len];
                IOUtils.readFully(in, small, len);
            } else {
                DataReader reader = new DataReader(in);
                char[] buff = new char[len];
                IOUtils.readFully(reader, buff, len);
                small = new String(buff).getBytes("UTF-8");
            }
            int magic = readInt();
            if (magic != TransferOutputStream.LOB_MAGIC) {
                throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
            }
            return ValueLob.createSmallLob(type, small, length);
        }
        case Value.ARRAY: {
            int len = readInt();
            Class<?> componentType = Object.class;
            if (len < 0) {
                len = -(len + 1);
                componentType = Utils.loadUserClass(readString());
            }
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue();
            }
            return ValueArray.get(componentType, list);
        }
        case Value.RESULT_SET: {
            SimpleResultSet rs = new SimpleResultSet();
            int columns = readInt();
            for (int i = 0; i < columns; i++) {
                rs.addColumn(readString(), readInt(), readInt(), readInt());
            }
            while (true) {
                if (!readBoolean()) {
                    break;
                }
                Object[] o = new Object[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = readValue().getObject();
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    /**
     * This class is backed by an input stream and supports reading values and
     * variable size data.
     */
    private static class DataReader extends Reader {

        private final InputStream in;

        /**
         * Create a new data reader.
         *
         * @param in the input stream
         */
        public DataReader(InputStream in) {
            this.in = in;
        }

        /**
         * Read a byte.
         *
         * @return the byte
         */
        private byte readByte() throws IOException {
            int x = in.read();
            if (x < 0) {
                throw new FastEOFException();
            }
            return (byte) x;
        }

        /**
         * Read one character from the input stream.
         *
         * @return the character
         */
        private char readChar() throws IOException {
            int x = readByte() & 0xff;
            if (x < 0x80) {
                return (char) x;
            } else if (x >= 0xe0) {
                return (char) (((x & 0xf) << 12) + ((readByte() & 0x3f) << 6) + (readByte() & 0x3f));
            } else {
                return (char) (((x & 0x1f) << 6) + (readByte() & 0x3f));
            }
        }

        @Override
        public void close() throws IOException {
            // ignore
        }

        @Override
        public int read(char[] buff, int off, int len) throws IOException {
            int i = 0;
            try {
                for (; i < len; i++) {
                    buff[i] = readChar();
                }
                return len;
            } catch (EOFException e) {
                return i;
            }
        }
    }

    /**
     * Constructing such an EOF exception is fast, because the stack trace is
     * not filled in. If used in a static context, this will also avoid
     * classloader memory leaks.
     */
    private static class FastEOFException extends EOFException {

        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Throwable fillInStackTrace() {
            return null;
        }
    }
}
