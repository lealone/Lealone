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
package org.lealone.net.netty;

import org.lealone.net.NetBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class NettyBuffer implements NetBuffer {

    private final ByteBuf buffer;

    public NettyBuffer(ByteBuf buff) {
        this.buffer = buff;
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    @Override
    public NettyBuffer appendBuffer(NetBuffer buff) {
        if (buff instanceof NettyBuffer) {
            this.buffer.writeBytes(((NettyBuffer) buff).getBuffer());
        }
        return this;
    }

    @Override
    public int length() {
        return buffer.writerIndex();
    }

    @Override
    public NettyBuffer slice(int start, int end) {
        return new NettyBuffer(buffer.slice(start, end - start));
    }

    @Override
    public NettyBuffer getBuffer(int start, int end) {
        byte[] bytes = new byte[end - start];
        buffer.getBytes(start, bytes, 0, end - start);
        ByteBuf subBuffer = Unpooled.unreleasableBuffer(Unpooled.buffer(bytes.length, Integer.MAX_VALUE))
                .writeBytes(bytes);
        return new NettyBuffer(subBuffer);
    }

    @Override
    public short getUnsignedByte(int pos) {
        return buffer.getUnsignedByte(pos);
    }

    @Override
    public NettyBuffer appendByte(byte b) {
        buffer.writeByte(b);
        return this;
    }

    @Override
    public NettyBuffer appendBytes(byte[] bytes, int offset, int len) {
        buffer.writeBytes(bytes, offset, len);
        return this;
    }

    @Override
    public NettyBuffer appendInt(int i) {
        buffer.writeInt(i);
        return this;
    }

    @Override
    public NettyBuffer setByte(int pos, byte b) {
        ensureWritable(pos, 1);
        buffer.setByte(pos, b);
        return this;
    }

    private void ensureWritable(int pos, int len) {
        int ni = pos + len;
        int cap = buffer.capacity();
        int over = ni - cap;
        if (over > 0) {
            buffer.writerIndex(cap);
            buffer.ensureWritable(over);
        }
        // We have to make sure that the writer index is always positioned on the last bit of data set in the buffer
        if (ni > buffer.writerIndex()) {
            buffer.writerIndex(ni);
        }
    }
}
