/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
