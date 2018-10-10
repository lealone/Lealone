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
import java.io.OutputStream;

public class NetBufferOutputStream extends OutputStream {

    protected final WritableChannel writableChannel;
    protected final int initialSizeHint;
    protected NetBuffer buffer;

    public NetBufferOutputStream(WritableChannel writableChannel, int initialSizeHint) {
        this.writableChannel = writableChannel;
        this.initialSizeHint = initialSizeHint;
        reset();
    }

    @Override
    public void write(int b) {
        buffer.appendByte((byte) b);
    }

    @Override
    public void write(byte b[], int off, int len) {
        buffer.appendBytes(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        NetBuffer old = buffer;
        reset();
        writableChannel.write(old);
        // 警告: 不能像下面这样用，调用write后会很快写数据到接收端，然后另一个线程很快又收到响应，
        // 在调用reset前又继续用原来的buffer写，从而导致产生非常难找的协议与并发问题，我就为这个问题痛苦排查过大半天。
        // writableChannel.write(buffer);
        // reset();
    }

    protected void reset() {
        buffer = writableChannel.getBufferFactory().createBuffer(initialSizeHint);
    }
}
