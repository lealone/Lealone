/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.common.util.MathUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.result.SimpleResultSet;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueBoolean;
import com.lealone.db.value.ValueByte;
import com.lealone.db.value.ValueBytes;
import com.lealone.db.value.ValueDataType;
import com.lealone.db.value.ValueDataTypeBase;
import com.lealone.db.value.ValueDate;
import com.lealone.db.value.ValueDecimal;
import com.lealone.db.value.ValueDouble;
import com.lealone.db.value.ValueEnum;
import com.lealone.db.value.ValueFloat;
import com.lealone.db.value.ValueInt;
import com.lealone.db.value.ValueJavaObject;
import com.lealone.db.value.ValueList;
import com.lealone.db.value.ValueLob;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueMap;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueResultSet;
import com.lealone.db.value.ValueSet;
import com.lealone.db.value.ValueShort;
import com.lealone.db.value.ValueString;
import com.lealone.db.value.ValueStringFixed;
import com.lealone.db.value.ValueStringIgnoreCase;
import com.lealone.db.value.ValueTime;
import com.lealone.db.value.ValueTimestamp;
import com.lealone.db.value.ValueUuid;

/**
 * @author H2 Group
 * @author zhh
 */
public class DataBuffer {

    private static final ValueDataTypeBase[] TYPES = new ValueDataTypeBase[Value.TYPE_COUNT];
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

    public static final int MAX_REUSE_CAPACITY = 4 * 1024 * 1024;

    /**
     * The minimum number of bytes to grow a buffer at a time.
     */
    public static final int MIN_GROW = 1024;

    /**
     * The data handler responsible for lob objects.
     */
    private final DataHandler handler;

    private ByteBuffer reuse; // = allocate(MIN_GROW);

    private ByteBuffer buff; // = reuse;

    private boolean direct;

    /**
     * Create a new buffer for the given handler.
     * The handler will decide what type of buffer is created.
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

    public static DataBuffer create() {
        return createDirect();
    }

    public static DataBuffer create(int capacity) {
        return createDirect(capacity);
    }

    public static DataBuffer createHeap() {
        return new DataBuffer(null, MIN_GROW, false);
    }

    public static DataBuffer createDirect(int capacity) {
        return new DataBuffer(null, capacity, true);
    }

    public static DataBuffer createDirect() {
        return new DataBuffer(null, MIN_GROW, true);
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

    public boolean getDirect() {
        return direct;
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

    public int getUnsignedByte() {
        return buff.get() & 0xff;
    }

    public DataBuffer slice(int start, int end) {
        return new DataBuffer(sliceByteBuffer(start, end));
    }

    public ByteBuffer sliceByteBuffer(int start, int end) {
        int pos = buff.position();
        int limit = buff.limit();
        buff.position(start);
        buff.limit(end);
        ByteBuffer newBuffer = buff.slice();
        buff.position(pos);
        buff.limit(limit);
        return newBuffer;
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

    public static ByteBuffer write(ValueDataType type, Object obj) {
        DataBuffer b = DataBuffer.create();
        type.write(b, obj);
        return b.getAndFlipBuffer();
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

    public ByteBuffer growCapacity(int plus) {
        grow(plus);
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

    public void writeValue(Value v) {
        writeValue(this, v);
    }

    // 通过ValueDataType写入硬盘的数据格式
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
                if (lob.isUseTableLobStorage()) {
                    buff.putVarInt(-2);
                } else {
                    buff.putVarInt(-3);
                }
                buff.putVarInt(lob.getTableId()).putVarLong(lob.getLobId())
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
        case Value.SET: {
            buff.put((byte) type);
            ValueSet vs = (ValueSet) v;
            Set<Value> set = vs.getSet();
            int size = set.size();
            Class<?> componentType = vs.getComponentType();
            if (componentType == Object.class) {
                putVarInt(size);
            } else {
                putVarInt(-(size + 1));
                writeString(buff, componentType.getName());
            }
            for (Value value : set) {
                writeValue(value);
            }
            break;
        }
        case Value.LIST: {
            buff.put((byte) type);
            ValueList vl = (ValueList) v;
            List<Value> list = vl.getList();
            int size = list.size();
            Class<?> componentType = vl.getComponentType();
            if (componentType == Object.class) {
                putVarInt(size);
            } else {
                putVarInt(-(size + 1));
                writeString(buff, componentType.getName());
            }
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.MAP: {
            buff.put((byte) type);
            ValueMap vm = (ValueMap) v;
            Map<Value, Value> map = vm.getMap();
            int size = map.size();
            Class<?> kType = vm.getKeyType();
            Class<?> vType = vm.getValueType();
            if (kType == Object.class && vType == Object.class) {
                putVarInt(size);
            } else {
                putVarInt(-(size + 1));
                writeString(buff, kType.getName());
                writeString(buff, vType.getName());
            }
            for (Entry<Value, Value> e : map.entrySet()) {
                writeValue(e.getKey());
                writeValue(e.getValue());
            }
            break;
        }
        case Value.ENUM: {
            buff.put((byte) type);
            putVarInt(v.getInt());
            break;
        }
        default:
            TYPES[type].writeValue(buff, v);
        }
    }

    private static void writeString(DataBuffer buff, String s) {
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

    public Value readValue() {
        return readValue(this.buff);
    }

    // 通过ValueDataType从硬盘把数据变成各种Value的子类
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
            } else if (smallLen == -3 || smallLen == -2) {
                int tableId = readVarInt(buff);
                long lobId = readVarLong(buff);
                long precision = readVarLong(buff);
                ValueLob lob = ValueLob.create(type, null, tableId, lobId, null, precision);
                if (smallLen == -3) {
                    lob.setUseTableLobStorage(false);
                } else {
                    lob.setUseTableLobStorage(true);
                }
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
        case Value.SET:
        case Value.LIST: {
            int size = readVarInt(buff);
            Class<?> componentType = Object.class;
            if (size < 0) {
                size = -(size + 1);
                componentType = Utils.loadUserClass(readString(buff));
            }
            Value[] values = new Value[size];
            for (int i = 0; i < size; i++) {
                values[i] = readValue(buff);
            }
            if (type == Value.LIST)
                return ValueList.get(componentType, values);
            else
                return ValueSet.get(componentType, values);
        }
        case Value.MAP: {
            int size = readVarInt(buff);
            Class<?> kType = Object.class, vType = Object.class;
            if (size < 0) {
                size = -(size + 1);
                kType = Utils.loadUserClass(readString(buff));
                vType = Utils.loadUserClass(readString(buff));
            }
            size = size * 2;
            Value[] values = new Value[size];
            for (int i = 0; i < size; i += 2) {
                values[i] = readValue(buff);
                values[i + 1] = readValue(buff);
            }
            return ValueMap.get(kType, vType, values);
        }
        case Value.ENUM: {
            int ordinal = readVarInt(buff);
            return ValueEnum.get(ordinal);
        }
        default:
            int type2 = ValueDataType.getTypeId(type);
            if (type2 >= TYPES.length)
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "type: " + type);
            return TYPES[type2].readValue(buff, type);
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
        DataBuffer d = new DataBuffer(null, 3 * Constants.IO_BUFFER_SIZE, false);
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
}
