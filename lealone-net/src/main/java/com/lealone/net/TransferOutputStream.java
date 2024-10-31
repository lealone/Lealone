/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.net;

import java.io.DataOutputStream;
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
import com.lealone.common.util.IOUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.DataBuffer;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.session.Session;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueDate;
import com.lealone.db.value.ValueList;
import com.lealone.db.value.ValueLob;
import com.lealone.db.value.ValueMap;
import com.lealone.db.value.ValueResultSet;
import com.lealone.db.value.ValueSet;
import com.lealone.db.value.ValueTime;
import com.lealone.db.value.ValueTimestamp;
import com.lealone.db.value.ValueUuid;
import com.lealone.server.protocol.PacketType;
import com.lealone.storage.page.PageKey;

/**
 * The transfer class is used to send Value objects.
 * It is used on both the client side, and on the server side.
 * 
 * @author H2 Group
 * @author zhh
 */
// 这个类的所有操作要在单个线程中进行
public class TransferOutputStream implements NetOutputStream {

    public static final byte REQUEST = 1;
    public static final byte RESPONSE = 2;

    private final DataOutputStream out;
    private final GlobalNetBufferOutputStream outBuffer;
    private Session session; // 每次写新的包时可以指定新的session

    public TransferOutputStream(WritableChannel writableChannel) {
        outBuffer = new GlobalNetBufferOutputStream(writableChannel, NetBuffer.BUFFER_SIZE);
        out = new DataOutputStream(outBuffer);
    }

    public DataOutputStream getDataOutputStream() {
        return out;
    }

    public TransferOutputStream writeRequestHeader(Session session, int packetId, PacketType packetType)
            throws IOException {
        this.session = session;
        startWrite(Session.STATUS_OK);
        writeByte(REQUEST).writeInt(packetId).writeInt(packetType.value).writeInt(session.getId());
        return this;
    }

    public TransferOutputStream writeRequestHeaderWithoutSessionId(int packetId, int packetType)
            throws IOException {
        startWrite(Session.STATUS_OK);
        writeByte(REQUEST).writeInt(packetId).writeInt(packetType);
        return this;
    }

    public TransferOutputStream writeResponseHeader(Session session, int packetId, int status)
            throws IOException {
        this.session = session;
        startWrite(status);
        writeByte(RESPONSE).writeInt(packetId).writeInt(status);
        return this;
    }

    private void startWrite(int status) {
        outBuffer.startWrite(status);
    }

    @Override
    public void setSSL(boolean ssl) {
        // this.ssl = ssl;
    }

    /**
     * Write pending changes.
     */
    public void flush() throws IOException {
        if (session != null) // 一些场景允许为null
            session.checkClosed();
        outBuffer.flush();
    }

    public void close() {
        outBuffer.buffer.getDataBuffer().close();
    }

    // 出错时可以重置
    public void reset() {
        outBuffer.buffer.reset();
    }

    /**
     * Write a boolean.
     *
     * @param x the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeBoolean(boolean x) throws IOException {
        out.writeByte((byte) (x ? 1 : 0));
        return this;
    }

    /**
     * Write a byte.
     *
     * @param x the value
     * @return itself
     */
    private TransferOutputStream writeByte(byte x) throws IOException {
        out.writeByte(x);
        return this;
    }

    /**
     * Write an int.
     *
     * @param x the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeInt(int x) throws IOException {
        out.writeInt(x);
        return this;
    }

    /**
     * Write a long.
     *
     * @param x the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeLong(long x) throws IOException {
        out.writeLong(x);
        return this;
    }

    /**
     * Write a double.
     *
     * @param i the value
     * @return itself
     */
    private TransferOutputStream writeDouble(double i) throws IOException {
        out.writeDouble(i);
        return this;
    }

    /**
     * Write a float.
     *
     * @param i the value
     * @return itself
     */
    private TransferOutputStream writeFloat(float i) throws IOException {
        out.writeFloat(i);
        return this;
    }

    /**
     * Write a string. The maximum string length is Integer.MAX_VALUE.
     *
     * @param s the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeString(String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            int len = s.length();
            out.writeInt(len);
            for (int i = 0; i < len; i++) {
                out.writeChar(s.charAt(i));
            }
        }
        return this;
    }

    /**
     * Write a byte buffer.
     *
     * @param data the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeByteBuffer(ByteBuffer data) throws IOException {
        if (data == null) {
            writeInt(-1);
        } else {
            if (data.hasArray()) {
                writeBytes(data.array(), data.arrayOffset(), data.limit());
            } else {
                byte[] bytes = new byte[data.limit()];
                data.get(bytes);
                writeBytes(bytes);
            }
        }
        return this;
    }

    /**
     * Write a byte array.
     *
     * @param data the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeBytes(byte[] data) throws IOException {
        if (data == null) {
            writeInt(-1);
        } else {
            writeInt(data.length);
            out.write(data);
        }
        return this;
    }

    /**
     * Write a number of bytes.
     *
     * @param buff the value
     * @param off the offset
     * @param len the length
     * @return itself
     */
    public TransferOutputStream writeBytes(byte[] buff, int off, int len) throws IOException {
        writeInt(len);
        out.write(buff, off, len);
        return this;
    }

    @Override
    public TransferOutputStream writePageKey(PageKey pk) throws IOException {
        writeValue((Value) pk.key);
        writeBoolean(pk.first);
        return this;
    }

    /**
     * Write a value.
     *
     * @param v the value
     */
    @Override
    public void writeValue(Value v) throws IOException {
        int type = v.getType();
        writeInt(type);
        switch (type) {
        case Value.NULL:
            break;
        case Value.BYTES:
        case Value.JAVA_OBJECT:
            writeBytes(v.getBytesNoCopy());
            break;
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.BOOLEAN:
            writeBoolean(v.getBoolean());
            break;
        case Value.BYTE:
            writeByte(v.getByte());
            break;
        case Value.DATE:
            writeLong(((ValueDate) v).getDateValue());
            break;
        case Value.TIME:
            writeLong(((ValueTime) v).getNanos());
            break;
        case Value.TIMESTAMP: {
            ValueTimestamp ts = (ValueTimestamp) v;
            writeLong(ts.getDateValue());
            writeLong(ts.getNanos());
            break;
        }
        case Value.DECIMAL:
            writeString(v.getString());
            break;
        case Value.DOUBLE:
            writeDouble(v.getDouble());
            break;
        case Value.FLOAT:
            writeFloat(v.getFloat());
            break;
        case Value.SHORT:
            writeInt(v.getShort());
            break;
        case Value.INT:
            writeInt(v.getInt());
            break;
        case Value.LONG:
            writeLong(v.getLong());
            break;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            writeString(v.getString());
            break;
        case Value.BLOB:
        case Value.CLOB: {
            ValueLob lob = (ValueLob) v;
            if (lob.isStored()) {
                writeLong(-1);
                writeInt(lob.getTableId());
                writeLong(lob.getLobId());
                writeBytes(calculateLobMac(lob));
                writeLong(lob.getPrecision());
                break;
            }
            long length = v.getPrecision();
            if (length < 0) {
                throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "length=" + length);
            }
            writeLong(length);
            if (type == Value.BLOB) {
                long written = IOUtils.copyAndCloseInput(v.getInputStream(), out);
                if (written != length) {
                    throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                            "length:" + length + " written:" + written);
                }
            } else {
                Reader reader = v.getReader();
                DataBuffer.copyString(reader, out);
            }
            writeInt(LOB_MAGIC);
            break;
        }
        case Value.ARRAY: {
            ValueArray va = (ValueArray) v;
            Value[] list = va.getList();
            int len = list.length;
            Class<?> componentType = va.getComponentType();
            if (componentType == Object.class) {
                writeInt(len);
            } else {
                writeInt(-(len + 1));
                writeString(componentType.getName());
            }
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.RESULT_SET: {
            try {
                ResultSet rs = ((ValueResultSet) v).getResultSet();
                rs.beforeFirst();
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                writeInt(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    writeString(meta.getColumnName(i + 1));
                    writeInt(meta.getColumnType(i + 1));
                    writeInt(meta.getPrecision(i + 1));
                    writeInt(meta.getScale(i + 1));
                }
                while (rs.next()) {
                    writeBoolean(true);
                    for (int i = 0; i < columnCount; i++) {
                        int t = DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1));
                        Value val = DataType.readValue(session, rs, i + 1, t);
                        writeValue(val);
                    }
                }
                writeBoolean(false);
                rs.beforeFirst();
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
            break;
        }
        case Value.SET: {
            ValueSet vs = (ValueSet) v;
            Set<Value> set = vs.getSet();
            int size = set.size();
            Class<?> componentType = vs.getComponentType();
            if (componentType == Object.class) {
                writeInt(size);
            } else {
                writeInt(-(size + 1));
                writeString(componentType.getName());
            }
            for (Value value : set) {
                writeValue(value);
            }
            break;
        }
        case Value.LIST: {
            ValueList vl = (ValueList) v;
            List<Value> list = vl.getList();
            int size = list.size();
            Class<?> componentType = vl.getComponentType();
            if (componentType == Object.class) {
                writeInt(size);
            } else {
                writeInt(-(size + 1));
                writeString(componentType.getName());
            }
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.MAP: {
            ValueMap vm = (ValueMap) v;
            Map<Value, Value> map = vm.getMap();
            int size = map.size();
            Class<?> kType = vm.getKeyType();
            Class<?> vType = vm.getValueType();
            if (kType == Object.class && vType == Object.class) {
                writeInt(size);
            } else {
                writeInt(-(size + 1));
                writeString(kType.getName());
                writeString(vType.getName());
            }
            for (Entry<Value, Value> e : map.entrySet()) {
                writeValue(e.getKey());
                writeValue(e.getValue());
            }
            break;
        }
        case Value.ENUM: {
            writeString(v.getString());
            writeInt(v.getInt());
            break;
        }
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    private static final int LOB_MAGIC = 0x1234;

    public static void verifyLobMagic(int magic) {
        if (magic != TransferOutputStream.LOB_MAGIC) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
        }
    }

    /**
     * Verify the HMAC.
     *
     * @param hmacData the message authentication code
     * @param lobId the lobId
     * @throws DbException if the HMAC does not match
     */
    public static int verifyLobMac(byte[] hmacData, long lobId) {
        long hmac = Utils.readLong(hmacData, 0);
        if ((lobId >> 32) != ((int) hmac)) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                    "Invalid lob hmac; possibly the connection was re-opened internally");
        }
        return (int) (hmac >> 32);
    }

    private static byte[] calculateLobMac(ValueLob lob) {
        int tableId = lob.getTableId();
        long lobId = lob.getLobId();
        if (lob.isUseTableLobStorage())
            tableId = -tableId;
        long hmac = (lobId >> 32) + (((long) tableId) << 32);
        byte[] hmacData = new byte[8];
        Utils.writeLong(hmacData, 0, hmac);
        return hmacData;
    }

    private static class GlobalNetBufferOutputStream extends OutputStream {

        private final GlobalWritableChannel channel;
        private final NetBuffer buffer;

        GlobalNetBufferOutputStream(WritableChannel writableChannel, int initialSizeHint) {
            channel = new GlobalWritableChannel(writableChannel, initialSizeHint);
            buffer = channel.getGlobalBuffer();
        }

        @Override
        public void write(int b) {
            buffer.appendByte((byte) b);
        }

        @Override
        public void write(byte b[], int off, int len) {
            buffer.appendBytes(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            int length = buffer.position() - channel.startPos - 4;
            writePacketLength(channel.startPos, length);
            channel.flush();
        }

        // 按java.io.DataInputStream.readInt()的格式写
        private void writePacketLength(int pos, int v) {
            buffer.setByte(pos, (byte) ((v >>> 24) & 0xFF));
            buffer.setByte(pos + 1, (byte) ((v >>> 16) & 0xFF));
            buffer.setByte(pos + 2, (byte) ((v >>> 8) & 0xFF));
            buffer.setByte(pos + 3, (byte) (v & 0xFF));
        }

        protected void startWrite(int status) {
            channel.startWrite(status);
            // 协议包头占4个字节，最后flush时再回填
            buffer.appendInt(0);
        }
    }

    // 外部插件也在使用这个类
    public static class GlobalWritableChannel {

        private final WritableChannel writableChannel;
        private final NetBuffer buffer;

        private int startPos;
        private boolean written;

        public GlobalWritableChannel(WritableChannel writableChannel) {
            this(writableChannel, NetBuffer.BUFFER_SIZE);
        }

        public GlobalWritableChannel(WritableChannel writableChannel, int initialSizeHint) {
            this.writableChannel = writableChannel;
            buffer = writableChannel.getBufferFactory().createBuffer(initialSizeHint,
                    writableChannel.getDataBufferFactory());
            buffer.setGlobal(true);
        }

        public NetBuffer getGlobalBuffer() {
            return buffer;
        }

        public ByteBuffer getByteBuffer() {
            return buffer.getByteBuffer();
        }

        public void allocate(int capacity) {
            buffer.getDataBuffer().checkCapacity(capacity);
        }

        public void flush() {
            // 不能立刻flip，因为全局buffer有可能后续的包会继续写入
            writableChannel.write(buffer);
            written = true;
        }

        public void startWrite(int status) {
            if (status == Session.STATUS_ERROR) {
                // 如果某个包写到一半出错了又写一个错误包，那需要把前面的覆盖掉
                if (!written && startPos != buffer.position()) {
                    buffer.position(startPos);
                    buffer.decrementPacketCount();
                }
            }
            written = false;
            // 全局buffer只需要记住下一个包的开始位置即可，会一直往后追加
            startPos = buffer.position();
            buffer.incrementPacketCount();
        }
    }
}
