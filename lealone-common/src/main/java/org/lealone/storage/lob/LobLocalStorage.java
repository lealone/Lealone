/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.lob;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DataHandler;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLob;

/**
 * This factory creates in-memory objects and temporary files.
 * It is used on the client side.
 * 
 * @author H2 Group
 * @author zhh
 */
public class LobLocalStorage implements LobStorage {

    private final DataHandler handler;
    private final LobReader lobReader;

    public LobLocalStorage(DataHandler handler) {
        this(handler, null);
    }

    public LobLocalStorage(DataHandler handler, LobReader lobReader) {
        this.handler = handler;
        this.lobReader = lobReader;
    }

    @Override
    public void init() {
        // nothing to do
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Value createBlob(InputStream in, long maxLength) {
        // need to use a temp file, because the input stream could come from
        // the same database, which would create a weird situation (trying
        // to read a block while writing something)
        return ValueLob.createTempBlob(in, maxLength, handler);
    }

    /**
     * Create a CLOB object.
     *
     * @param reader the reader
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    @Override
    public Value createClob(Reader reader, long maxLength) {
        // need to use a temp file, because the input stream could come from
        // the same database, which would create a weird situation (trying
        // to read a block while writing something)
        return ValueLob.createTempClob(reader, maxLength, handler);
    }

    @Override
    public ValueLob copyLob(ValueLob old, int tableId, long length) {
        throw new UnsupportedOperationException();
    }

    /**
    * Get the input stream for the given lob.
    *
    * @param lob the lob
    * @param hmac the message authentication code (for remote input streams)
    * @param byteCount the number of bytes to read, or -1 if not known
    * @return the stream
    */
    @Override
    public InputStream getInputStream(ValueLob lob, byte[] hmac, long byteCount) throws IOException {
        if (byteCount < 0) {
            byteCount = Long.MAX_VALUE;
        }
        return new BufferedInputStream(new LobInputStream(lobReader, lob, hmac, byteCount));
    }

    @Override
    public void setTable(ValueLob lob, int tableIdSessionVariable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAllForTable(int tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLob(ValueLob lob) {
        // not stored in the database
    }

    /**
     * An input stream that reads from a remote LOB.
     */
    private static class LobInputStream extends InputStream {

        private final LobReader lobReader;

        /**
         * The lob id.
         */
        private final long lobId;

        private final byte[] hmac;

        /**
         * The position.
         */
        private long pos;

        /**
         * The remaining bytes in the lob.
         */
        private long remainingBytes;

        LobInputStream(LobReader lobReader, ValueLob lob, byte[] hmac, long byteCount) {
            this.lobReader = lobReader;
            this.lobId = lob.getLobId();
            this.hmac = hmac;
            remainingBytes = byteCount;
        }

        @Override
        public int read() throws IOException {
            byte[] buff = new byte[1];
            int len = read(buff, 0, 1);
            return len < 0 ? len : (buff[0] & 255);
        }

        @Override
        public int read(byte[] buff) throws IOException {
            return read(buff, 0, buff.length);
        }

        @Override
        public int read(byte[] buff, int off, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            length = (int) Math.min(length, remainingBytes);
            if (length == 0) {
                return -1;
            }
            try {
                length = lobReader.readLob(lobId, hmac, pos, buff, off, length);
            } catch (DbException e) {
                throw DbException.convertToIOException(e);
            }
            remainingBytes -= length;
            if (length == 0) {
                return -1;
            }
            pos += length;
            return length;
        }

        @Override
        public long skip(long n) {
            remainingBytes -= n;
            pos += n;
            return n;
        }
    }

    public static interface LobReader {
        /**
        * Read from a lob.
        *
        * @param lobId the lob
        * @param hmac the message authentication code
        * @param offset the offset within the lob
        * @param buff the target buffer
        * @param off the offset within the target buffer
        * @param length the number of bytes to read
        * @return the number of bytes read
        */
        int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length);
    }
}
