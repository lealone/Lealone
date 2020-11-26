/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.SimpleResultSet;
import org.lealone.db.value.DataType;
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
import org.lealone.storage.type.StorageDataType;
import org.lealone.storage.type.StorageDataTypeBase;

/**
 * @author H2 Group
 * @author zhh
 */
public class DataBuffer implements AutoCloseable {

    public static final StorageDataTypeBase[] TYPES = new StorageDataTypeBase[Value.TYPE_COUNT];
    static {
        TYPES[Value.NULL] = ValueNull.type;
        TYPES[Value.BOOLEAN] = ValueBoolean.type;
        TYPES[Value.BYTE] = ValueByte.type;
        TYPES[Value.SHORT] = ValueShort.type;
        TYPES[Value.INT] = ValueInt.type;
        TYPES[Value.LONG] = ValueLong.type;
        TYPES[Value.FLOAT] = ValueFloat.type;
        TYPES[Value.DOUBLE] = ValueDouble.type;
        TYPES[Value.DECIMAL] = ValueDecimal.type;
        TYPES[Value.STRING] = ValueString.type;
        TYPES[Value.UUID] = ValueUuid.type;
        TYPES[Value.DATE] = ValueDate.type;
        TYPES[Value.TIME] = ValueTime.type;
        TYPES[Value.TIMESTAMP] = ValueTimestamp.type;
    }

    /**
     * The length of an integer value.
     */
    public static final int LENGTH_INT = 4;

    private static final int MAX_REUSE_CAPACITY = 4 * 1024 * 1024;

    /**
     * The minimum number of bytes to grow a buffer at a time.
     */
    private static final int MIN_GROW = 1024;

    /**
     * The data handler responsible for lob objects.
     */
    private final DataHandler handler;

    private ByteBuffer reuse; // = allocate(MIN_GROW);

    private ByteBuffer buff; // = reuse;

    private boolean direct;

    /**
     * Create a new buffer for the given handler. The
     * handler will decide what type of buffer is created.
     *
     * @param handler the data handler
     * @param capacity the initial capacity of the buffer
     * @return the buffer
     */
    public static DataBuffer create(DataHandler handler, int capacity) {
        return new DataBuffer(handler, capacity);
    }

    public static DataBuffer create(DataHandler handler, int capacity, boolean direct) {
        return new DataBuffer(handler, capacity, direct);
    }

    public static DataBuffer create(DataHandler handler) {
        return new DataBuffer(handler, MIN_GROW);
    }

    public static DataBuffer create(int capacity) {
        return new DataBuffer(null, capacity);
    }

    public static DataBuffer create(ByteBuffer buff) {
        return new DataBuffer(buff);
    }

    protected DataBuffer() {
        this(null, MIN_GROW);
    }

    protected DataBuffer(DataHandler handler, int capacity) {
        this(handler, capacity, true);
    }

    protected DataBuffer(DataHandler handler, int capacity, boolean direct) {
        this.handler = handler;
        this.direct = direct;
        reuse = allocate(capacity);
        buff = reuse;
    }

    protected DataBuffer(ByteBuffer buff) {
        this.handler = null;
        this.buff = reuse = buff;
    }

    public DataHandler getHandler() {
        return handler;
    }

    /**
     * Set the position to 0.
     */
    public void reset() {
        buff.position(0);
        // buff.clear();
    }

    /**
     * Write a variable size integer.
     *
     * @param x the value
     * @return this
     */
    public DataBuffer putVarInt(int x) {
        DataUtils.writeVarInt(ensureCapacity(5), x);
        return this;
    }

    /**
     * Write a variable size long.
     *
     * @param x the value
     * @return this
     */
    public DataBuffer putVarLong(long x) {
        DataUtils.writeVarLong(ensureCapacity(10), x);
        return this;
    }

    /**
     * Write the characters of a string in a format similar to UTF-8.
     *
     * @param s the string
     * @param len the number of characters to write
     * @return this
     */
    public DataBuffer putStringData(String s, int len) {
        ByteBuffer b = ensureCapacity(3 * len);
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                b.put((byte) c);
            } else if (c >= 0x800) {
                b.put((byte) (0xe0 | (c >> 12)));
                b.put((byte) (((c >> 6) & 0x3f)));
                b.put((byte) (c & 0x3f));
            } else {
                b.put((byte) (0xc0 | (c >> 6)));
                b.put((byte) (c & 0x3f));
            }
        }
        return this;
    }

    /**
     * Put a byte.
     *
     * @param x the value
     * @return this
     */
    public DataBuffer put(byte x) {
        ensureCapacity(1).put(x);
        return this;
    }

    /**
     * Put a character.
     *
     * @param x the value
     * @return this
     */
    public DataBuffer putChar(char x) {
        ensureCapacity(2).putChar(x);
        return this;
    }

    /**
     * Put a short.
     *
     * @param x the value
     * @return this
     */
    public DataBuffer putShort(short x) {
        ensureCapacity(2).putShort(x);
        return this;
    }

    /**
     * Put an integer.
     *
     * @param x the value
     * @return this
     */
    public DataBuffer putInt(int x) {
        ensureCapacity(4).putInt(x);
        return this;
    }

    public int getInt() {
        return buff.getInt();
    }

    public short getUnsignedByte(int pos) {
        return (short) (buff.get(pos) & 0xff);
    }

    public DataBuffer slice(int start, int end) {
        int pos = buff.position();
        int limit = buff.limit();
        buff.position(start);
        buff.limit(end);
        ByteBuffer newBuffer = buff.slice();
        buff.position(pos);
        buff.limit(limit);
        return new DataBuffer(newBuffer);
    }

    public DataBuffer getBuffer(int start, int end) {
        byte[] bytes = new byte[end - start];
        // 不能直接这样用，get的javadoc是错的，start应该是bytes的位置
        // buff.get(bytes, start, end - start);
        int pos = buff.position();
        buff.position(start);
        buff.get(bytes, 0, end - start);
        buff.position(pos);
        ByteBuffer newBuffer = ByteBuffer.wrap(bytes);
        return new DataBuffer(newBuffer);
    }

    /**
     * Copy a number of bytes to the given buffer from the current position. The
     * current position is incremented accordingly.
     *
     * @param dst the output buffer
     * @param off the offset in the output buffer
     * @param len the number of bytes to copy
     */
    public void read(byte[] dst, int off, int len) {
        this.buff.get(dst, off, len);
    }

    public byte readByte() {
        return buff.get();
    }

    /**
     * Set the current read / write position.
     *
     * @param pos the new position
     */
    public void setPos(int pos) {
        buff.position(pos);
    }

    /**
     * Put a long.
     *
     * @param x the value
     * @return this
     */
    public DataBuffer putLong(long x) {
        ensureCapacity(8).putLong(x);
        return this;
    }

    public long getLong() {
        return buff.getLong();
    }

    /**
     * Put a float.
     *
     * @param x the value
     * @return this
     */
    public DataBuffer putFloat(float x) {
        ensureCapacity(4).putFloat(x);
        return this;
    }

    /**
     * Put a double.
     *
     * @param x the value
     * @return this
     */
    public DataBuffer putDouble(double x) {
        ensureCapacity(8).putDouble(x);
        return this;
    }

    /**
     * Put a byte array.
     *
     * @param bytes the value
     * @return this
     */
    public DataBuffer put(byte[] bytes) {
        ensureCapacity(bytes.length).put(bytes);
        return this;
    }

    /**
     * Put a byte array.
     *
     * @param bytes the value
     * @param offset the source offset
     * @param length the number of bytes
     * @return this
     */
    public DataBuffer put(byte[] bytes, int offset, int length) {
        ensureCapacity(length).put(bytes, offset, length);
        return this;
    }

    /**
     * Put the contents of a byte buffer.
     *
     * @param src the source buffer
     * @return this
     */
    public DataBuffer put(ByteBuffer src) {
        ensureCapacity(src.remaining()).put(src);
        return this;
    }

    /**
     * Set the limit, possibly growing the buffer.
     *
     * @param newLimit the new limit
     * @return this
     */
    public DataBuffer limit(int newLimit) {
        ensureCapacity(newLimit - buff.position()).limit(newLimit);
        return this;
    }

    /**
     * Get the capacity.
     *
     * @return the capacity
     */
    public int capacity() {
        return buff.capacity();
    }

    /**
     * Set the position.
     *
     * @param newPosition the new position
     * @return the new position
     */
    public DataBuffer position(int newPosition) {
        buff.position(newPosition);
        return this;
    }

    /**
     * Get the limit.
     *
     * @return the limit
     */
    public int limit() {
        return buff.limit();
    }

    /**
     * Get the current position.
     *
     * @return the position
     */
    public int position() {
        return buff.position();
    }

    /**
     * Copy the data into the destination array.
     *
     * @param dst the destination array
     * @return this
     */
    public DataBuffer get(byte[] dst) {
        buff.get(dst);
        return this;
    }

    /**
     * Update an integer at the given index.
     *
     * @param index the index
     * @param value the value
     * @return this
     */
    public DataBuffer putInt(int index, int value) {
        buff.putInt(index, value);
        return this;
    }

    /**
     * Update a short at the given index.
     *
     * @param index the index
     * @param value the value
     * @return this
     */
    public DataBuffer putShort(int index, short value) {
        buff.putShort(index, value);
        return this;
    }

    /**
     * Update a byte at the given index.
     *
     * @param index the index
     * @param value the value
     * @return this
     */
    public DataBuffer putByte(int index, byte value) {
        buff.put(index, value);
        return this;
    }

    /**
     * Clear the buffer after use.
     *
     * @return this
     */
    public DataBuffer clear() {
        if (buff.limit() > MAX_REUSE_CAPACITY) {
            buff = reuse;
        } else if (buff != reuse) {
            reuse = buff;
        }
        buff.clear();
        return this;
    }

    /**
     * Get the byte buffer.
     *
     * @return the byte buffer
     */
    public ByteBuffer getBuffer() {
        return buff;
    }

    public ByteBuffer getAndFlipBuffer() {
        buff.flip();
        return buff;
    }

    public ByteBuffer getAndCopyBuffer() {
        buff.flip();
        ByteBuffer value = allocate(buff.limit());
        value.put(buff);
        value.flip();
        return value;
    }

    public ByteBuffer write(StorageDataType type, Object obj) {
        type.write(this, obj);
        ByteBuffer buff = getAndFlipBuffer();

        ByteBuffer v = allocate(buff.limit());
        v.put(buff);
        v.flip();
        return v;
    }

    /**
     * Check if there is still enough capacity in the buffer.
     * This method extends the buffer if required.
     *
     * @param plus the number of additional bytes required
     */
    public void checkCapacity(int plus) {
        ensureCapacity(plus);
    }

    private ByteBuffer ensureCapacity(int len) {
        if (buff.remaining() < len) {
            grow(len);
        }
        return buff;
    }

    private void grow(int additional) {
        int pos = buff.position();
        ByteBuffer temp = buff;
        int needed = additional - temp.remaining();
        // grow at least MIN_GROW
        long grow = Math.max(needed, MIN_GROW);
        // grow at least 50% of the current size
        grow = Math.max(temp.capacity() / 2, grow);
        // the new capacity is at most Integer.MAX_VALUE
        int newCapacity = (int) Math.min(Integer.MAX_VALUE, temp.capacity() + grow);
        if (newCapacity < needed) {
            throw new OutOfMemoryError("Capacity: " + newCapacity + " needed: " + needed);
        }
        try {
            buff = allocate(newCapacity);
        } catch (OutOfMemoryError e) {
            throw new OutOfMemoryError("Capacity: " + newCapacity);
        }
        // temp.flip();
        temp.position(0);
        buff.put(temp);
        buff.position(pos);
        if (newCapacity <= MAX_REUSE_CAPACITY) {
            reuse = buff;
        }
    }

    /**
     * Fill up the buffer with empty space and an (initially empty) checksum
     * until the size is a multiple of Constants.FILE_BLOCK_SIZE.
     */
    public void fillAligned() {
        int position = buff.position();
        // 0..6 > 8, 7..14 > 16, 15..22 > 24, ...
        int len = MathUtils.roundUpInt(position + 2, Constants.FILE_BLOCK_SIZE);
        ensureCapacity(len - position);
        buff.position(len);
    }

    /**
     * Get the byte array used for this page.
     *
     * @return the byte array
     */
    public byte[] getBytes() {
        return buff.array();
    }

    /**
     * Get the current write position of this buffer, which is the current length.
     *
     * @return the length
     */
    public int length() {
        return buff.position();
    }

    @Override
    public void close() {
        DataBufferPool.offer(this);
    }

    public static DataBuffer create() {
        return DataBufferPool.poll();
    }

    public void writeValue(Value v) {
        writeValue(this, v);
    }

    private void writeValue(DataBuffer buff, Value v) {
        int type = v.getType();
        switch (type) {
        case Value.BYTES:
        case Value.JAVA_OBJECT: {
            byte[] b = v.getBytesNoCopy();
            buff.put((byte) type).putVarInt(b.length).put(b);
            break;
        }
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            buff.put((byte) type);
            writeString(buff, v.getString());
            break;
        case Value.BLOB:
        case Value.CLOB: {
            buff.put((byte) type);
            ValueLob lob = (ValueLob) v;
            byte[] small = lob.getSmall();
            if (small == null) {
                buff.putVarInt(-3).putVarInt(lob.getTableId()).putVarLong(lob.getLobId())
                        .putVarLong(lob.getPrecision());
            } else {
                buff.putVarInt(small.length).put(small);
            }
            break;
        }
        case Value.ARRAY: {
            buff.put((byte) type);
            ValueArray va = (ValueArray) v;
            Value[] list = va.getList();
            int len = list.length;
            Class<?> componentType = va.getComponentType();
            if (componentType == Object.class) {
                putVarInt(len);
            } else {
                putVarInt(-(len + 1));
                writeString(buff, componentType.getName());
            }
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.RESULT_SET: {
            buff.put((byte) type);
            try {
                ResultSet rs = ((ValueResultSet) v).getResultSet();
                rs.beforeFirst();
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                buff.putVarInt(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    writeString(buff, meta.getColumnName(i + 1));
                    buff.putVarInt(meta.getColumnType(i + 1)).putVarInt(meta.getPrecision(i + 1))
                            .putVarInt(meta.getScale(i + 1));
                }
                while (rs.next()) {
                    buff.put((byte) 1);
                    for (int i = 0; i < columnCount; i++) {
                        int t = DataType.getValueTypeFromResultSet(meta, i + 1);
                        Value val = DataType.readValue(null, rs, i + 1, t);
                        writeValue(buff, val);
                    }
                }
                buff.put((byte) 0);
                rs.beforeFirst();
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            break;
        }
        // case Value.GEOMETRY: {
        // byte[] b = v.getBytes();
        // int len = b.length;
        // buff.put((byte) type).putVarInt(len).put(b);
        // break;
        // }
        default:
            type = StorageDataType.getTypeId(type);
            TYPES[type].writeValue(buff, v);
            // DbException.throwInternalError("type=" + v.getType());
        }
    }

    private static void writeString(DataBuffer buff, String s) {
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

    public Value readValue() {
        return readValue(this.buff);
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    public static Value readValue(ByteBuffer buff) {
        int type = buff.get() & 255;
        switch (type) {
        case Value.BYTES:
        case Value.JAVA_OBJECT: {
            int len = readVarInt(buff);
            byte[] b = DataUtils.newBytes(len);
            buff.get(b, 0, len);
            if (type == Value.BYTES) {
                return ValueBytes.getNoCopy(b);
            } else {
                // return ValueJavaObject.getNoCopy(null, b, handler);
                return ValueJavaObject.getNoCopy(null, b);
            }
        }
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString(buff));
        case Value.STRING_FIXED:
            return ValueStringFixed.get(readString(buff));
        case Value.BLOB:
        case Value.CLOB: {
            int smallLen = readVarInt(buff);
            if (smallLen >= 0) {
                byte[] small = DataUtils.newBytes(smallLen);
                buff.get(small, 0, smallLen);
                return ValueLob.createSmallLob(type, small);
            } else if (smallLen == -3) {
                int tableId = readVarInt(buff);
                long lobId = readVarLong(buff);
                long precision = readVarLong(buff);
                // ValueLobDb lob = ValueLobDb.create(type, handler, tableId, lobId, null, precision);
                ValueLob lob = ValueLob.create(type, null, tableId, lobId, null, precision);
                return lob;
            } else {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "lob type: " + smallLen);
            }
        }
        case Value.ARRAY: {
            int len = readVarInt(buff);
            Class<?> componentType = Object.class;
            if (len < 0) {
                len = -(len + 1);
                componentType = Utils.loadUserClass(readString(buff));
            }
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue(buff);
            }
            return ValueArray.get(componentType, list);
        }
        case Value.RESULT_SET: {
            SimpleResultSet rs = new SimpleResultSet();
            rs.setAutoClose(false);
            int columns = readVarInt(buff);
            for (int i = 0; i < columns; i++) {
                rs.addColumn(readString(buff), readVarInt(buff), readVarInt(buff), readVarInt(buff));
            }
            while (true) {
                if (buff.get() == 0) {
                    break;
                }
                Object[] o = new Object[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = readValue(buff).getObject();
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        default:
            int type2 = StorageDataType.getTypeId(type);
            return TYPES[type2].readValue(buff, type);
        // throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "type: " + type);
        }

    }

    private static int readVarInt(ByteBuffer buff) {
        return DataUtils.readVarInt(buff);
    }

    private static long readVarLong(ByteBuffer buff) {
        return DataUtils.readVarLong(buff);
    }

    private static String readString(ByteBuffer buff) {
        int len = readVarInt(buff);
        return DataUtils.readString(buff, len);
    }

    /**
     * Copy a String from a reader to an output stream.
     *
     * @param source the reader
     * @param target the output stream
     */
    public static void copyString(Reader source, OutputStream target) throws IOException {
        char[] buff = new char[Constants.IO_BUFFER_SIZE];
        DataBuffer d = new DataBuffer(null, 3 * Constants.IO_BUFFER_SIZE);
        while (true) {
            int l = source.read(buff);
            if (l < 0) {
                break;
            }
            d.writeStringWithoutLength(buff, l);
            target.write(d.getBytes(), 0, d.length());
            d.reset();
        }
    }

    private void writeStringWithoutLength(char[] chars, int len) {
        int p = this.buff.position();
        byte[] buff = this.buff.array();
        for (int i = 0; i < len; i++) {
            int c = chars[i];
            if (c < 0x80) {
                buff[p++] = (byte) c;
            } else if (c >= 0x800) {
                buff[p++] = (byte) (0xe0 | (c >> 12));
                buff[p++] = (byte) (((c >> 6) & 0x3f));
                buff[p++] = (byte) (c & 0x3f);
            } else {
                buff[p++] = (byte) (0xc0 | (c >> 6));
                buff[p++] = (byte) (c & 0x3f);
            }
        }
        this.buff.position(p);
    }

    private ByteBuffer allocate(int capacity) {
        return direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    }

    private static class DataBufferPool {
        // 不要求精确
        private static int poolSize;
        private static final int maxPoolSize = 20;
        private static final int capacity = 4 * 1024 * 1024;
        private static final ConcurrentLinkedQueue<DataBuffer> dataBufferPool = new ConcurrentLinkedQueue<>();

        public static DataBuffer poll() {
            DataBuffer writeBuffer = dataBufferPool.poll();
            if (writeBuffer == null)
                writeBuffer = new DataBuffer();
            else {
                writeBuffer.clear();
                poolSize--;
            }
            return writeBuffer;
        }

        public static void offer(DataBuffer writeBuffer) {
            if (poolSize < maxPoolSize && writeBuffer.capacity() <= capacity) {
                poolSize++;
                dataBufferPool.offer(writeBuffer);
            }
        }
    }
}
