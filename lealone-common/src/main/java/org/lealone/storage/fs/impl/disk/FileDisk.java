/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.fs.impl.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;

import org.lealone.db.SysProperties;
import org.lealone.storage.fs.impl.FileBase;

/**
 * Uses java.io.RandomAccessFile to access a file.
 */
class FileDisk extends FileBase {

    private final RandomAccessFile file;
    private final String name;
    private final boolean readOnly;

    FileDisk(String fileName, String mode) throws FileNotFoundException {
        this.file = new RandomAccessFile(fileName, mode);
        this.name = fileName;
        this.readOnly = mode.equals("r");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        String m = SysProperties.SYNC_METHOD;
        if ("".equals(m)) {
            // do nothing
        } else if ("sync".equals(m)) {
            file.getFD().sync();
        } else if ("force".equals(m)) {
            file.getChannel().force(true);
        } else if ("forceFalse".equals(m)) {
            file.getChannel().force(false);
        } else {
            file.getFD().sync();
        }
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        // compatibility with JDK FileChannel#truncate
        if (readOnly) {
            throw new NonWritableChannelException();
        }
        if (newLength < file.length()) {
            file.setLength(newLength);
        }
        return this;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return file.getChannel().tryLock(position, size, shared);
    }

    @Override
    public void implCloseChannel() throws IOException {
        file.close();
    }

    @Override
    public long position() throws IOException {
        return file.getFilePointer();
    }

    @Override
    public long size() throws IOException {
        return file.length();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int len = file.read(dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());
        if (len > 0) {
            dst.position(dst.position() + len);
        }
        return len;
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        file.seek(pos);
        return this;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        file.write(src.array(), src.arrayOffset() + src.position(), len);
        src.position(src.position() + len);
        return len;
    }

    @Override
    public String toString() {
        return name;
    }
}
