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
package org.lealone.net.nio;

import java.nio.ByteBuffer;

import org.lealone.net.NetBuffer;

public class NioBuffer implements NetBuffer {

    private ByteBuffer buffer;

    public NioBuffer(ByteBuffer buff) {
        this.buffer = buff;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public NioBuffer appendBuffer(NetBuffer buff) {
        if (buff instanceof NioBuffer) {
            ByteBuffer newBuff = ((NioBuffer) buff).getBuffer();
            if (buffer.limit() == 0) {
                buffer = newBuff;
            } else {
                ByteBuffer tmp = ByteBuffer.allocate(buffer.limit() + (newBuff.limit() - newBuff.position()));
                tmp.put(buffer);
                tmp.put(newBuff);
                tmp.flip();
                buffer = tmp;
            }
        }
        return this;
    }

    @Override
    public int length() {
        int pos = buffer.position();
        if (pos > 0)
            return pos;
        else
            return buffer.limit();
    }

    @Override
    public NioBuffer slice(int start, int end) {
        int pos = this.buffer.position();
        int limit = this.buffer.limit();
        this.buffer.position(start);
        this.buffer.limit(end);
        ByteBuffer buffer = this.buffer.slice();
        this.buffer.position(pos);
        this.buffer.limit(limit);
        return new NioBuffer(buffer);
    }

    @Override
    public NioBuffer getBuffer(int start, int end) {
        byte[] bytes = new byte[end - start];
        // 不能直接这样用，get的javadoc是错的，start应该是bytes的位置
        // buffer.get(bytes, start, end - start);
        int pos = buffer.position();
        buffer.position(start);
        buffer.get(bytes, 0, end - start);
        buffer.position(pos);
        ByteBuffer newBuffer = ByteBuffer.wrap(bytes);
        return new NioBuffer(newBuffer);
    }

    @Override
    public short getUnsignedByte(int pos) {
        return (short) (buffer.get(pos) & 0xff);
    }

    @Override
    public NioBuffer appendByte(byte b) {
        buffer.put(b);
        return this;
    }

    @Override
    public NioBuffer appendBytes(byte[] bytes, int offset, int len) {
        buffer.put(bytes, offset, len);
        return this;
    }

    @Override
    public NioBuffer appendInt(int i) {
        buffer.putInt(i);
        return this;
    }

    @Override
    public NioBuffer setByte(int pos, byte b) {
        buffer.put(pos, b);
        return this;
    }
}
