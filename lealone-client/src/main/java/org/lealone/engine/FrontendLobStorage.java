/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.engine;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.lealone.message.DbException;
import org.lealone.value.Value;
import org.lealone.value.ValueLobDb;

/**
 * This factory creates in-memory objects and temporary files. It is used on the
 * client side.
 */
public class FrontendLobStorage implements LobStorageInterface {

    private final DataHandler handler;

    public FrontendLobStorage(DataHandler handler) {
        this.handler = handler;
    }

    @Override
    public void removeLob(ValueLobDb lob) {
        // not stored in the database
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
    public InputStream getInputStream(ValueLobDb lob, byte[] hmac, long byteCount) throws IOException {
        if (byteCount < 0) {
            byteCount = Long.MAX_VALUE;
        }
        return new BufferedInputStream(new FrontendLobStorageInputStream(handler, lob, hmac, byteCount));
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public ValueLobDb copyLob(ValueLobDb old, int tableId, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTable(ValueLobDb lob, int tableIdSessionVariable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAllForTable(int tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value createBlob(InputStream in, long maxLength) {
        // need to use a temp file, because the input stream could come from
        // the same database, which would create a weird situation (trying
        // to read a block while writing something)
        return ValueLobDb.createTempBlob(in, maxLength, handler);
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
        return ValueLobDb.createTempClob(reader, maxLength, handler);
    }

    @Override
    public void init() {
        // nothing to do
    }

    /**
     * An input stream that reads from a remote LOB.
     */
    private static class FrontendLobStorageInputStream extends InputStream {

        /**
         * The data handler.
         */
        private final DataHandler handler;

        /**
         * The lob id.
         */
        private final long lob;

        private final byte[] hmac;

        /**
         * The position.
         */
        private long pos;

        /**
         * The remaining bytes in the lob.
         */
        private long remainingBytes;

        FrontendLobStorageInputStream(DataHandler handler, ValueLobDb lob, byte[] hmac, long byteCount) {
            this.handler = handler;
            this.lob = lob.getLobId();
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
                length = handler.readLob(lob, hmac, pos, buff, off, length);
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
}
