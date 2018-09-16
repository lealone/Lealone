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
package org.lealone.net;

import java.io.IOException;
import java.io.InputStream;

public class NetBufferInputStream extends InputStream {

    protected final NetBuffer buffer;
    protected final int size;
    protected int pos;

    public NetBufferInputStream(NetBuffer buffer) {
        this.buffer = buffer;
        size = buffer.length();
    }

    @Override
    public int available() throws IOException {
        return size - pos;
    }

    @Override
    public int read() throws IOException {
        return buffer.getUnsignedByte(pos++);
    }
}
