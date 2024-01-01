/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.storage.fs;

import java.io.OutputStream;

import com.lealone.common.compress.CompressTool;
import com.lealone.db.Constants;
import com.lealone.db.DataBuffer;
import com.lealone.db.DataHandler;

/**
 * An output stream that is backed by a file storage.
 */
public class FileStorageOutputStream extends OutputStream {

    private FileStorage fileStorage;
    private final CompressTool compress;
    private final String compressionAlgorithm;
    private final DataBuffer page;
    private final byte[] buffer = { 0 };

    public FileStorageOutputStream(FileStorage fs, DataHandler handler, String compressionAlgorithm) {
        this.fileStorage = fs;
        if (compressionAlgorithm != null) {
            this.compress = CompressTool.getInstance();
            this.compressionAlgorithm = compressionAlgorithm;
        } else {
            this.compress = null;
            this.compressionAlgorithm = null;
        }
        page = DataBuffer.create(handler, Constants.FILE_BLOCK_SIZE, false); // 不能用direct byte buffer
    }

    @Override
    public void write(int b) {
        buffer[0] = (byte) b;
        write(buffer);
    }

    @Override
    public void write(byte[] buff) {
        write(buff, 0, buff.length);
    }

    @Override
    public void write(byte[] buff, int off, int len) {
        if (len > 0) {
            page.reset();
            if (compress != null) {
                if (off != 0 || len != buff.length) {
                    byte[] b2 = new byte[len];
                    System.arraycopy(buff, off, b2, 0, len);
                    buff = b2;
                    off = 0;
                }
                int uncompressed = len;
                buff = compress.compress(buff, compressionAlgorithm);
                len = buff.length;
                page.putInt(len);
                page.putInt(uncompressed);
                page.put(buff, off, len);
            } else {
                page.putInt(len);
                page.put(buff, off, len);
            }
            page.fillAligned();
            fileStorage.write(page.getBytes(), 0, page.length());
        }
    }

    @Override
    public void close() {
        if (fileStorage != null) {
            try {
                fileStorage.close();
            } finally {
                fileStorage = null;
            }
        }
    }
}
