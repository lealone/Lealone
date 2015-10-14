/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.New;
import org.lealone.common.value.Value;
import org.lealone.common.value.ValueLobDb;
import org.lealone.db.Constants;
import org.lealone.db.DataHandler;
import org.lealone.storage.btree.BTreeMap;

/**
 * This class stores LOB objects in the database, in maps. This is the back-end
 * i.e. the server side of the LOB storage.
 */
public class AOLobStorage implements LobStorage {

    private static final boolean TRACE = false;

    private final DataHandler dataHandler;

    private boolean init;

    private final Object nextLobIdSync = new Object();
    private long nextLobId;

    /**
     * The lob metadata map. It contains the mapping from the lob id
     * (which is a long) to the stream store id (which is a byte array).
     *
     * Key: lobId (long)
     * Value: { streamStoreId (byte[]), tableId (int),
     * byteCount (long), hash (long) }.
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
    private BTreeMap<Object[], Boolean> refMap;

    /**
     * The stream store data map.
     *
     * Key: stream store block id (long).
     * Value: data (byte[]).
     */
    private BTreeMap<Long, byte[]> dataMap;

    private StreamStorage streamStore;
    private final AOStorage storage;

    public AOLobStorage(DataHandler dataHandler) {
        this.dataHandler = dataHandler;
        storage = new AOStorage(null);
    }

    @Override
    public void init() {
        if (init) {
            return;
        }
        init = true;
        lobMap = storage.openBTreeMap("lobMap");
        refMap = storage.openBTreeMap("lobRef");
        dataMap = storage.openBTreeMap("lobData");
        streamStore = new StreamStorage(dataMap);
        // garbage collection of the last blocks
        if (storage.isReadOnly()) {
            return;
        }
        if (dataMap.isEmpty()) {
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
            lastUsedKey = streamStore.getMaxBlockKey(id);
            if (lastUsedKey >= 0) {
                break;
            }
            lobId = lobMap.floorKey(lobId);
        }
        // delete all blocks that are newer
        while (true) {
            Long last = dataMap.lastKey();
            if (last == null || last <= lastUsedKey) {
                break;
            }
            dataMap.remove(last);
        }
    }

    @Override
    public Value createBlob(InputStream in, long maxLength) {
        init();
        int type = Value.BLOB;
        if (maxLength < 0) {
            maxLength = Long.MAX_VALUE;
        }
        int max = (int) Math.min(maxLength, dataHandler.getMaxLengthInplaceLob());
        try {
            if (max != 0 && max < Integer.MAX_VALUE) {
                BufferedInputStream b = new BufferedInputStream(in, max);
                b.mark(max);
                byte[] small = new byte[max];
                int len = IOUtils.readFully(b, small, max);
                if (len < max) {
                    if (len < small.length) {
                        small = Arrays.copyOf(small, len);
                    }
                    return ValueLobDb.createSmallLob(type, small);
                }
                b.reset();
                in = b;
            }
            return createLob(in, type);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    @Override
    public Value createClob(Reader reader, long maxLength) {
        init();
        int type = Value.CLOB;
        if (maxLength < 0) {
            maxLength = Long.MAX_VALUE;
        }
        int max = (int) Math.min(maxLength, dataHandler.getMaxLengthInplaceLob());
        try {
            if (max != 0 && max < Integer.MAX_VALUE) {
                BufferedReader b = new BufferedReader(reader, max);
                b.mark(max);
                char[] small = new char[max];
                int len = IOUtils.readFully(b, small, max);
                if (len < max) {
                    if (len < small.length) {
                        small = Arrays.copyOf(small, len);
                    }
                    byte[] utf8 = new String(small, 0, len).getBytes(Constants.UTF8);
                    return ValueLobDb.createSmallLob(type, utf8);
                }
                b.reset();
                reader = b;
            }
            CountingReaderInputStream in = new CountingReaderInputStream(reader, maxLength);
            ValueLobDb lob = createLob(in, type);
            // the length is not correct
            lob = ValueLobDb.create(type, dataHandler, lob.getTableId(), lob.getLobId(), null, in.getLength());
            return lob;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private ValueLobDb createLob(InputStream in, int type) throws IOException {
        byte[] streamStoreId;
        try {
            streamStoreId = streamStore.put(in);
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
        long lobId = generateLobId();
        long length = streamStore.length(streamStoreId);
        int tableId = LobStorage.TABLE_TEMP;
        Object[] value = new Object[] { streamStoreId, tableId, length, 0 };
        lobMap.put(lobId, value);
        Object[] key = new Object[] { streamStoreId, lobId };
        refMap.put(key, Boolean.TRUE);
        ValueLobDb lob = ValueLobDb.create(type, dataHandler, tableId, lobId, null, length);
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
    public boolean isReadOnly() {
        return storage.isReadOnly();
    }

    @Override
    public ValueLobDb copyLob(ValueLobDb old, int tableId, long length) {
        init();
        int type = old.getType();
        long oldLobId = old.getLobId();
        long oldLength = old.getPrecision();
        if (oldLength != length) {
            throw DbException.throwInternalError("Length is different");
        }
        Object[] value = lobMap.get(oldLobId);
        value = Arrays.copyOf(value, value.length);
        byte[] streamStoreId = (byte[]) value[0];
        long lobId = generateLobId();
        value[1] = tableId;
        lobMap.put(lobId, value);
        Object[] key = new Object[] { streamStoreId, lobId };
        refMap.put(key, Boolean.TRUE);
        ValueLobDb lob = ValueLobDb.create(type, dataHandler, tableId, lobId, null, length);
        if (TRACE) {
            trace("copy " + old.getTableId() + "/" + old.getLobId() + " > " + tableId + "/" + lobId);
        }
        return lob;
    }

    @Override
    public InputStream getInputStream(ValueLobDb lob, byte[] hmac, long byteCount) throws IOException {
        init();
        Object[] value = lobMap.get(lob.getLobId());
        if (value == null) {
            throw DbException.throwInternalError("Lob not found: " + lob.getLobId());
        }
        byte[] streamStoreId = (byte[]) value[0];
        return streamStore.get(streamStoreId);
    }

    @Override
    public void setTable(ValueLobDb lob, int tableId) {
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
        init();
        if (storage.isClosed()) {
            return;
        }
        // this might not be very efficient -
        // to speed it up, we would need yet another map
        ArrayList<Long> list = New.arrayList();
        for (Entry<Long, Object[]> e : lobMap.entrySet()) {
            Object[] value = e.getValue();
            int t = (Integer) value[1];
            if (t == tableId) {
                list.add(e.getKey());
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
    public void removeLob(ValueLobDb lob) {
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
            streamStore.remove(streamStoreId);
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

        private final CharsetEncoder encoder = Constants.UTF8.newEncoder().onMalformedInput(CodingErrorAction.REPLACE)
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
         * The number of characters read so far (but there might still be some bytes
         * in the buffer).
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
}
