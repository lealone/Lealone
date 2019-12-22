package org.lealone.server.handler;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream with a position.
 */
public class CachedInputStream extends FilterInputStream {

    private static final ByteArrayInputStream DUMMY = new ByteArrayInputStream(new byte[0]);
    private long pos;

    public CachedInputStream(InputStream in) {
        super(in == null ? DUMMY : in);
        if (in == null) {
            pos = -1;
        }
    }

    @Override
    public int read(byte[] buff, int off, int len) throws IOException {
        len = super.read(buff, off, len);
        if (len > 0) {
            pos += len;
        }
        return len;
    }

    @Override
    public int read() throws IOException {
        int x = in.read();
        if (x >= 0) {
            pos++;
        }
        return x;
    }

    @Override
    public long skip(long n) throws IOException {
        n = super.skip(n);
        if (n > 0) {
            pos += n;
        }
        return n;
    }

    public long getPos() {
        return pos;
    }
}