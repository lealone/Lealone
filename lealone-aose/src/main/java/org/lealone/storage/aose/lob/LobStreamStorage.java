/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.aose.lob;

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

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.IOUtils;
import org.lealone.db.Constants;
import org.lealone.db.DataHandler;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLob;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.lob.LobStorage;

/**
 * This class stores LOB objects in the database, in maps.
 * This is the back-end i.e. the server side of the LOB storage.
 * 
 * @author H2 Group
 * @author zhh
 */
// 把所有的大对象(BLOB/CLOB)变成流，然后通过三个BTreeMap配合完成对大对象的存储
public class LobStreamStorage implements LobStorage {

    private static final boolean TRACE = false;

    private final DataHandler dataHandler;
    private final AOStorage storage;

    private final Object nextLobIdSync = new Object();
    private long nextLobId;

    /**
     * The lob metadata map. It contains the mapping from the lob id(which is a long) 
     * to the stream store id (which is a byte array).
     *
     * Key: lobId (long)
     * Value: { streamStoreId (byte[]), tableId (int), byteCount (long), hash (long) }. //最后两个未使用
     */
    private BTreeMap<Long, Object[]> lobMap;

    /**
     * The reference map. It is used to remove data from the stream store: if no
     * more entries for the given streamStoreId exist, the data is removed from
     * the stream store.
     *
     * Key: { streamStoreId (byte[]), lobId (long) }.
     * Value: true (boolean).
     */
    private BTreeMap<Object[], Boolean> refMap; // 调用copyLob时会有多个lob引用同一个streamStoreId

    // 这个字段才是实际存放大对象字节流的，上面两个只是放引用
    private LobStreamMap lobStreamMap;

    public LobStreamStorage(DataHandler dataHandler, Storage storage) {
        this.dataHandler = dataHandler;
        this.storage = (AOStorage) storage;
    }

    public LobStreamMap getLobStreamMap() {
        return lobStreamMap;
    }

    @Override
    public void init() {
        if (lobMap == null)
            lazyInit();
    }

    @Override
    public void save() {
        if (lobMap == null)
            return;
        lobMap.save();
        refMap.save();
        storage.openBTreeMap("lobData").save();
    }

    // 不是每个数据库都用到大对象字段的，所以只有实际用到时才创建相应的BTreeMap
    private synchronized void lazyInit() {
        if (lobMap != null)
            return;
        lobMap = storage.openBTreeMap("lobMap");
        refMap = storage.openBTreeMap("lobRef");
        lobStreamMap = new LobStreamMap(storage.openBTreeMap("lobData"));

        // garbage collection of the last blocks
        if (storage.isReadOnly()) {
            return;
        }
        if (lobStreamMap.isEmpty()) {
            return;
        }
        // search the last referenced block
        // (a lob may not have any referenced blocks if data is kept inline,
        // so we need to loop)
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
        // don't re-use block ids, except at the very end
        Long last = lobStreamMap.lastKey();
        if (last != null) {
            lobStreamMap.setNextKey(last + 1);
        }
    }

    @Override
    public boolean isReadOnly() {
        return storage.isReadOnly();
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
        int type = Value.CLOB;
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
                return ValueLob.createSmallLob(type, utf8);
            }
            if (maxLength < 0) {
                maxLength = Long.MAX_VALUE;
            }
            CountingReaderInputStream in = new CountingReaderInputStream(reader, maxLength);
            ValueLob lob = createLob(in, type);
            // the length is not correct
            lob = ValueLob.create(type, dataHandler, lob.getTableId(), lob.getLobId(), null,
                    in.getLength());
            return lob;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private ValueLob createLob(InputStream in, int type) throws IOException {
        byte[] streamStoreId = lobStreamMap.put(in);
        long lobId = generateLobId();
        long length = lobStreamMap.length(streamStoreId);
        int tableId = LobStorage.TABLE_TEMP;
        Object[] value = { streamStoreId, tableId };
        lobMap.put(lobId, value);
        Object[] key = { streamStoreId, lobId };
        refMap.put(key, Boolean.TRUE);
        ValueLob lob = ValueLob.create(type, dataHandler, tableId, lobId, null, length);
        if (TRACE) {
            trace("create " + tableId + "/" + lobId);
        }
        return lob;
    }

    private long generateLobId() {
        synchronized (nextLobIdSync) {
            if (nextLobId == 0) {
                Long id = lobMap.lastKey();
                nextLobId = id == null ? 1 : id + 1;
            }
            return nextLobId++;
        }
    }

    @Override
    public ValueLob copyLob(ValueLob old, int tableId, long length) {
        init();
        int type = old.getType();
        long oldLobId = old.getLobId();
        long oldLength = old.getPrecision();
        if (oldLength != length) {
            throw DbException.getInternalError("Length is different");
        }
        Object[] value = lobMap.get(oldLobId);
        value = Arrays.copyOf(value, value.length);
        byte[] streamStoreId = (byte[]) value[0];
        long lobId = generateLobId();
        value[1] = tableId;
        lobMap.put(lobId, value);
        Object[] key = new Object[] { streamStoreId, lobId };
        refMap.put(key, Boolean.TRUE);
        ValueLob lob = ValueLob.create(type, dataHandler, tableId, lobId, null, length);
        if (TRACE) {
            trace("copy " + old.getTableId() + "/" + old.getLobId() + " > " + tableId + "/" + lobId);
        }
        return lob;
    }

    @Override
    public InputStream getInputStream(ValueLob lob, byte[] hmac, long byteCount) throws IOException {
        init();
        Object[] value = lobMap.get(lob.getLobId());
        if (value == null) {
            throw DbException.getInternalError("Lob not found: " + lob.getLobId());
        }
        byte[] streamStoreId = (byte[]) value[0];
        return lobStreamMap.get(streamStoreId);
    }

    @Override
    public void setTable(ValueLob lob, int tableId) {
        init();
        long lobId = lob.getLobId();
        Object[] value = lobMap.remove(lobId);
        if (TRACE) {
            trace("move " + lob.getTableId() + "/" + lob.getLobId() + " > " + tableId + "/" + lobId);
        }
        value[1] = tableId;
        lobMap.put(lobId, value);
    }

    @Override
    public void removeAllForTable(int tableId) {
        if (storage.isClosed()) {
            return;
        }
        init();
        // this might not be very efficient -
        // to speed it up, we would need yet another map
        ArrayList<Long> list = new ArrayList<>();
        StorageMapCursor<Long, Object[]> cursor = lobMap.cursor();
        while (cursor.hasNext()) {
            cursor.next();
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
            removeAllForTable(LobStorage.TABLE_RESULT);
        }
    }

    @Override
    public void removeLob(ValueLob lob) {
        init();
        int tableId = lob.getTableId();
        long lobId = lob.getLobId();
        removeLob(tableId, lobId);
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
        Object[] key = new Object[] { streamStoreId, lobId };
        refMap.remove(key);
        // check if there are more entries for this streamStoreId
        key = new Object[] { streamStoreId, 0L };
        value = refMap.ceilingKey(key);
        boolean hasMoreEntries = false;
        if (value != null) {
            byte[] s2 = (byte[]) value[0];
            if (Arrays.equals(streamStoreId, s2)) {
                hasMoreEntries = true;
            }
        }
        if (!hasMoreEntries) {
            lobStreamMap.remove(streamStoreId);
        }
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
