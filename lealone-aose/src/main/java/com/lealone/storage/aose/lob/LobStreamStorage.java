/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.storage.aose.lob;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.ZipOutputStream;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.IOUtils;
import com.lealone.db.Constants;
import com.lealone.db.DataHandler;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLob;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageMapCursor;
import com.lealone.storage.aose.AOStorage;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.lob.LobStorage;
import com.lealone.storage.type.StorageDataType;
import com.lealone.storage.type.StorageDataTypeFactory;
import com.lealone.transaction.TransactionEngine;

/**
 * This class stores LOB objects in the database, in maps.
 * This is the back-end i.e. the server side of the LOB storage.
 * 
 * @author H2 Group
 * @author zhh
 */
// 把所有的大对象(BLOB/CLOB)变成流，然后通过2个BTreeMap配合完成对大对象的存储
public class LobStreamStorage implements LobStorage, com.lealone.transaction.TransactionEngine.GcTask {

    private static final boolean TRACE = false;

    private final DataHandler dataHandler;
    private final AOStorage storage;

    /**
     * The lob metadata map. It contains the mapping from the lob id(which is a long) 
     * to the stream store id (which is a byte array).
     *
     * Key: lobId (long)
     * Value: { streamStoreId (byte[]), tableId (int) }.
     */
    private BTreeMap<Long, Object[]> lobMap;

    // 这个字段才是实际存放大对象字节流的
    private LobStreamMap lobStreamMap;

    public LobStreamStorage(DataHandler dataHandler, Storage storage) {
        this.dataHandler = dataHandler;
        this.storage = (AOStorage) storage;
    }

    public LobStreamMap getLobStreamMap() {
        init();
        return lobStreamMap;
    }

    @Override
    public void save() {
        if (lobMap != null) {
            lobMap.save();
            lobStreamMap.save();
        }
    }

    @Override
    public void gc(TransactionEngine te) {
        if (lobMap != null) {
            lobMap.gc(te);
            lobStreamMap.gc(te);
        }
    }

    @Override
    public void close() {
        if (lobMap != null) {
            lobMap.close();
            lobStreamMap.close();
        }
    }

    @Override
    public void backupTo(String baseDir, ZipOutputStream out, Long lastDate) {
        if (storage != null) {
            init();
            storage.backupTo(baseDir, out, lastDate);
        }
    }

    private void init() {
        if (lobMap == null)
            lazyInit();
    }

    // 不是每个数据库都用到大对象字段的，所以只有实际用到时才创建相应的BTreeMap
    private synchronized void lazyInit() {
        if (lobMap != null)
            return;
        StorageDataType longType = StorageDataTypeFactory.getLongType();
        BTreeMap<Long, Object[]> lobMap = storage.openBTreeMap("lobMap", longType, null, null);
        lobStreamMap = new LobStreamMap(storage.openBTreeMap("lobData", longType, null, null));
        if (!lobStreamMap.isEmpty()) {
            // search the last referenced block
            // (a lob may not have any referenced blocks if data is kept inline, so we need to loop)
            long lastUsedKey = -1;
            Long lobId = lobMap.lastKey();
            while (lobId != null) {
                Object[] v = lobMap.get(lobId);
                byte[] id = (byte[]) v[0];
                lastUsedKey = lobStreamMap.getMaxBlockKey(id);
                if (lastUsedKey >= 0) {
                    break;
                }
                lobId = lobMap.floorKey(lobId);
            }
            if (TRACE) {
                trace("lastUsedKey=" + lastUsedKey);
            }
            // delete all blocks that are newer
            while (true) {
                Long last = lobStreamMap.lastKey();
                if (last == null || last <= lastUsedKey) {
                    break;
                }
                if (TRACE) {
                    trace("gc " + last);
                }
                lobStreamMap.remove(last);
            }
        }
        this.lobMap = lobMap;
    }

    @Override
    public ValueLob createBlob(InputStream in, long maxLength) {
        init();
        try {
            if (maxLength != -1 && maxLength <= dataHandler.getMaxLengthInplaceLob()) {
                byte[] small = new byte[(int) maxLength];
                int len = IOUtils.readFully(in, small);
                if (len < small.length) {
                    small = Arrays.copyOf(small, len);
                }
                return ValueLob.createSmallLob(Value.BLOB, small);
            }
            if (maxLength != -1) {
                in = new RangeInputStream(in, 0L, maxLength);
            }
            return createLob(in, Value.BLOB);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    @Override
    public ValueLob createClob(Reader reader, long maxLength) {
        init();
        try {
            // we multiple by 3 here to get the worst-case size in bytes
            if (maxLength != -1 && maxLength * 3 <= dataHandler.getMaxLengthInplaceLob()) {
                char[] small = new char[(int) maxLength];
                int len = IOUtils.readFully(reader, small);
                if (len < small.length) {
                    small = Arrays.copyOf(small, len);
                }
                byte[] utf8 = new String(small, 0, len).getBytes(StandardCharsets.UTF_8);
                if (utf8.length > dataHandler.getMaxLengthInplaceLob()) {
                    throw new IllegalStateException("len > maxinplace, " + utf8.length + " > "
                            + dataHandler.getMaxLengthInplaceLob());
                }
                return ValueLob.createSmallLob(Value.CLOB, utf8);
            }
            if (maxLength < 0) {
                maxLength = Long.MAX_VALUE;
            }
            CountingReaderInputStream in = new CountingReaderInputStream(reader, maxLength);
            return createLob(in, Value.CLOB);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private ValueLob createLob(InputStream in, int type) throws IOException {
        byte[] streamStoreId = lobStreamMap.put(in);
        int tableId = LobStorage.TABLE_TEMP;
        Object[] value = { streamStoreId, tableId };
        long lobId = lobMap.append(value);
        long length;
        if (in instanceof CountingReaderInputStream)
            length = ((CountingReaderInputStream) in).getLength();
        else
            length = lobStreamMap.length(streamStoreId);
        if (TRACE) {
            trace("create " + tableId + "/" + lobId);
        }
        return ValueLob.create(type, dataHandler, tableId, lobId, null, length);
    }

    @Override
    public InputStream getInputStream(ValueLob lob, byte[] hmac, long byteCount) throws IOException {
        init();
        Object[] value = lobMap.get(lob.getLobId());
        if (value == null) {
            throw DbException.getInternalError("Lob not found: " + lob.getLobId());
        }
        byte[] streamStoreId = (byte[]) value[0];
        return lobStreamMap.getInputStream(streamStoreId);
    }

    @Override
    public void setTable(ValueLob lob, int tableId) {
        init();
        long lobId = lob.getLobId();
        Object[] value = lobMap.get(lobId);
        value[1] = tableId;
        if (TRACE) {
            trace("move " + lob.getTableId() + "/" + lob.getLobId() + " > " + tableId + "/" + lobId);
        }
        lobMap.markDirty(lobId);
    }

    @Override
    public void removeAllForTable(int tableId) {
        if (storage.isClosed()) {
            return;
        }
        init();
        if (dataHandler.isTableLobStorage()) {
            lobMap.clear();
            lobStreamMap.clear();
            return;
        }
        // this might not be very efficient -
        // to speed it up, we would need yet another map
        ArrayList<Long> list = new ArrayList<>();
        StorageMapCursor<Long, Object[]> cursor = lobMap.cursor();
        while (cursor.next()) {
            Object[] value = cursor.getValue();
            int t = (Integer) value[1];
            if (t == tableId) {
                list.add(cursor.getKey());
            }
        }
        for (long lobId : list) {
            removeLob(tableId, lobId);
        }
        if (tableId == LobStorage.TABLE_ID_SESSION_VARIABLE) {
            removeAllForTable(LobStorage.TABLE_TEMP);
        }
    }

    @Override
    public void removeLob(ValueLob lob) {
        init();
        removeLob(lob.getTableId(), lob.getLobId());
    }

    private void removeLob(int tableId, long lobId) {
        if (TRACE) {
            trace("remove " + tableId + "/" + lobId);
        }
        Object[] value = lobMap.remove(lobId);
        if (value == null) {
            // already removed
            return;
        }
        byte[] streamStoreId = (byte[]) value[0];
        lobStreamMap.remove(streamStoreId);
    }

    private static void trace(String op) {
        System.out.println(Thread.currentThread().getName() + " LOB " + op);
    }

    /**
     * An input stream that reads the data from a reader.
     */
    private static class CountingReaderInputStream extends InputStream {

        private final Reader reader;

        private final CharBuffer charBuffer = CharBuffer.allocate(Constants.IO_BUFFER_SIZE);

        private final CharsetEncoder encoder = Constants.UTF8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);

        private ByteBuffer byteBuffer = ByteBuffer.allocate(0);
        private long length;
        private long remaining;

        CountingReaderInputStream(Reader reader, long maxLength) {
            this.reader = reader;
            this.remaining = maxLength;
        }

        @Override
        public int read(byte[] buff, int offset, int len) throws IOException {
            if (!fetch()) {
                return -1;
            }
            len = Math.min(len, byteBuffer.remaining());
            byteBuffer.get(buff, offset, len);
            return len;
        }

        @Override
        public int read() throws IOException {
            if (!fetch()) {
                return -1;
            }
            return byteBuffer.get() & 255;
        }

        private boolean fetch() throws IOException {
            if (byteBuffer != null && byteBuffer.remaining() == 0) {
                fillBuffer();
            }
            return byteBuffer != null;
        }

        private void fillBuffer() throws IOException {
            int len = (int) Math.min(charBuffer.capacity() - charBuffer.position(), remaining);
            if (len > 0) {
                len = reader.read(charBuffer.array(), charBuffer.position(), len);
            }
            if (len > 0) {
                remaining -= len;
            } else {
                len = 0;
                remaining = 0;
            }
            length += len;
            charBuffer.limit(charBuffer.position() + len);
            charBuffer.rewind();
            byteBuffer = ByteBuffer.allocate(Constants.IO_BUFFER_SIZE);
            boolean end = remaining == 0;
            encoder.encode(charBuffer, byteBuffer, end);
            if (end && byteBuffer.position() == 0) {
                // EOF
                byteBuffer = null;
                return;
            }
            byteBuffer.flip();
            charBuffer.compact();
            charBuffer.flip();
            charBuffer.position(charBuffer.limit());
        }

        /**
         * The number of characters read so far (but there might still be some bytes in the buffer).
         *
         * @return the number of characters
         */
        public long getLength() {
            return length;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    /**
    * Input stream that reads only a specified range from the source stream.
    */
    private static class RangeInputStream extends FilterInputStream {
        private long limit;

        /**
         * Creates new instance of range input stream.
         *
         * @param in
         *            source stream
         * @param offset
         *            offset of the range
         * @param limit
         *            length of the range
         * @throws IOException
         *             on I/O exception during seeking to the specified offset
         */
        public RangeInputStream(InputStream in, long offset, long limit) throws IOException {
            super(in);
            this.limit = limit;
            IOUtils.skipFully(in, offset);
        }

        @Override
        public int read() throws IOException {
            if (limit <= 0) {
                return -1;
            }
            int b = in.read();
            if (b >= 0) {
                limit--;
            }
            return b;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            if (limit <= 0) {
                return -1;
            }
            if (len > limit) {
                len = (int) limit;
            }
            int cnt = in.read(b, off, len);
            if (cnt > 0) {
                limit -= cnt;
            }
            return cnt;
        }

        @Override
        public long skip(long n) throws IOException {
            if (n > limit) {
                n = (int) limit;
            }
            n = in.skip(n);
            limit -= n;
            return n;
        }

        @Override
        public int available() throws IOException {
            int cnt = in.available();
            if (cnt > limit) {
                return (int) limit;
            }
            return cnt;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public void mark(int readlimit) {
        }

        @Override
        public synchronized void reset() throws IOException {
            throw new IOException("mark/reset not supported");
        }

        @Override
        public boolean markSupported() {
            return false;
        }
    }
}
