/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.storage.fs;

import java.io.IOException;
import java.io.InputStream;

import com.lealone.common.compress.CompressTool;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.Constants;
import com.lealone.db.DataBuffer;
import com.lealone.db.DataHandler;

/**
 * An input stream that is backed by a file storage.
 */
public class FileStorageInputStream extends InputStream {

    private FileStorage fileStorage;
    private final boolean alwaysClose;
    private final CompressTool compress;
    private final DataBuffer page;
    private final boolean raw;
    private int remainingInBuffer;
    private boolean endOfFile;

    public FileStorageInputStream(FileStorage fileStorage, DataHandler handler, boolean compression,
            boolean alwaysClose) {
        this(fileStorage, handler, compression, alwaysClose, false);
    }

    public FileStorageInputStream(FileStorage fileStorage, DataHandler handler, boolean compression,
            boolean alwaysClose, boolean raw) {
        this.fileStorage = fileStorage;
        this.alwaysClose = alwaysClose;
        this.raw = raw;
        if (compression) {
            compress = CompressTool.getInstance();
        } else {
            compress = null;
        }
        int capacity = raw ? Constants.IO_BUFFER_SIZE : Constants.FILE_BLOCK_SIZE;
        page = DataBuffer.create(handler, capacity, false); // 不能用direct byte buffer
        try {
            fillBuffer();
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileStorage.getFileName());
        }
    }

    @Override
    public int available() {
        return remainingInBuffer <= 0 ? 0 : remainingInBuffer;
    }

    @Override
    public int read() throws IOException {
        fillBuffer();
        if (endOfFile) {
            return -1;
        }
        int i = page.readByte() & 0xff;
        remainingInBuffer--;
        return i;
    }

    @Override
    public int read(byte[] buff) throws IOException {
        return read(buff, 0, buff.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int read = 0;
        while (len > 0) {
            int r = readBlock(b, off, len);
            if (r < 0) {
                break;
            }
            read += r;
            off += r;
            len -= r;
        }
        return read == 0 ? -1 : read;
    }

    private int readBlock(byte[] buff, int off, int len) throws IOException {
        fillBuffer();
        if (endOfFile) {
            return -1;
        }
        int l = Math.min(remainingInBuffer, len);
        page.read(buff, off, l);
        remainingInBuffer -= l;
        return l;
    }

    private void fillBuffer() throws IOException {
        if (remainingInBuffer > 0 || endOfFile) {
            return;
        }
        page.reset();
        if (raw) {
            remainingInBuffer = (int) Math.min(fileStorage.length() - fileStorage.getFilePointer(),
                    Constants.IO_BUFFER_SIZE);
            if (remainingInBuffer <= 0) {
                endOfFile = true;
            } else {
                fileStorage.readFully(page.getBytes(), 0, remainingInBuffer);
            }
            return;
        }
        fileStorage.openFile();
        if (fileStorage.length() == fileStorage.getFilePointer()) {
            close();
            return;
        }
        fileStorage.readFully(page.getBytes(), 0, Constants.FILE_BLOCK_SIZE);
        page.reset();
        remainingInBuffer = page.getInt();
        if (remainingInBuffer < 0) {
            close();
            return;
        }
        page.checkCapacity(remainingInBuffer);
        // get the length to rea
        if (compress != null) {
            page.checkCapacity(DataBuffer.LENGTH_INT);
            page.getInt();
        }
        page.setPos(page.length() + remainingInBuffer);
        page.fillAligned();
        int len = page.length() - Constants.FILE_BLOCK_SIZE;
        page.reset();
        page.getInt();
        fileStorage.readFully(page.getBytes(), Constants.FILE_BLOCK_SIZE, len);
        page.reset();
        page.getInt();
        if (compress != null) {
            int uncompressed = page.getInt();
            byte[] buff = DataUtils.newBytes(remainingInBuffer);
            page.read(buff, 0, remainingInBuffer);
            page.reset();
            page.checkCapacity(uncompressed);
            CompressTool.expand(buff, page.getBytes(), 0);
            remainingInBuffer = uncompressed;
        }
        if (alwaysClose) {
            fileStorage.closeFile();
        }
    }

    @Override
    public void close() {
        if (raw)
            return;
        if (fileStorage != null) {
            try {
                fileStorage.close();
                endOfFile = true;
            } finally {
                fileStorage = null;
            }
        }
    }

    @Override
    protected void finalize() {
        close();
    }
}
