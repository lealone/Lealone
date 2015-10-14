/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.fs;

import java.io.OutputStream;

import org.lealone.common.compress.CompressTool;
import org.lealone.db.Constants;
import org.lealone.db.Data;
import org.lealone.db.DataHandler;

/**
 * An output stream that is backed by a file storage.
 */
public class FileStorageOutputStream extends OutputStream {
    private FileStorage fileStorage;
    private final Data page;
    private final String compressionAlgorithm;
    private final CompressTool compress;
    private final byte[] buffer = { 0 };

    public FileStorageOutputStream(FileStorage fileStorage, DataHandler handler, String compressionAlgorithm) {
        this.fileStorage = fileStorage;
        if (compressionAlgorithm != null) {
            this.compress = CompressTool.getInstance();
            this.compressionAlgorithm = compressionAlgorithm;
        } else {
            this.compress = null;
            this.compressionAlgorithm = null;
        }
        page = Data.create(handler, Constants.FILE_BLOCK_SIZE);
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
                page.checkCapacity(2 * Data.LENGTH_INT + len);
                page.writeInt(len);
                page.writeInt(uncompressed);
                page.write(buff, off, len);
            } else {
                page.checkCapacity(Data.LENGTH_INT + len);
                page.writeInt(len);
                page.write(buff, off, len);
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
