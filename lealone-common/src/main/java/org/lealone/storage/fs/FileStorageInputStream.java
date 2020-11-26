/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.fs;

import java.io.IOException;
import java.io.InputStream;

import org.lealone.common.compress.CompressTool;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.Constants;
import org.lealone.db.DataBuffer;
import org.lealone.db.DataHandler;

/**
 * An input stream that is backed by a file storage.
 */
public class FileStorageInputStream extends InputStream {

    private FileStorage fileStorage;
    private final DataBuffer page;
    private int remainingInBuffer;
    private final CompressTool compress;
    private boolean endOfFile;
    private final boolean alwaysClose;

    public FileStorageInputStream(FileStorage fileStorage, DataHandler handler, boolean compression,
            boolean alwaysClose) {
        this.fileStorage = fileStorage;
        this.alwaysClose = alwaysClose;
        if (compression) {
            compress = CompressTool.getInstance();
        } else {
            compress = null;
        }
        page = DataBuffer.create(handler, Constants.FILE_BLOCK_SIZE, false); // 不能用direct byte buffer
        try {
            if (fileStorage.length() <= FileStorage.HEADER_LENGTH) {
                close();
            } else {
                fillBuffer();
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileStorage.name);
        }
    }

    @Override
    public int available() {
        return remainingInBuffer <= 0 ? 0 : remainingInBuffer;
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

}
