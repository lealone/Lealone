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

import org.lealone.db.DataBuffer;
import org.lealone.net.NetBuffer;

public class NioBuffer implements NetBuffer {

    private DataBuffer dataBuffer;

    public NioBuffer(DataBuffer dataBuffer) {
        this.dataBuffer = dataBuffer;
    }

    public ByteBuffer getByteBuffer() {
        return dataBuffer.getAndFlipBuffer();
    }

    @Override
    public NioBuffer appendBuffer(NetBuffer buff) {
        if (buff instanceof NioBuffer) {
            DataBuffer newDataBuffer = ((NioBuffer) buff).dataBuffer;
            if (dataBuffer.limit() == 0) {
                dataBuffer = newDataBuffer;
            } else {
                DataBuffer tmp = DataBuffer
                        .create(dataBuffer.limit() + (newDataBuffer.limit() - newDataBuffer.position()));
                tmp.put(dataBuffer.getBuffer());
                tmp.put(newDataBuffer.getBuffer());
                tmp.getBuffer().flip();
                dataBuffer = tmp;
                // dataBuffer.position(dataBuffer.limit()).put(newDataBuffer.getBuffer());
                // dataBuffer.getBuffer().flip();
            }
        }
        return this;
    }

    @Override
    public int length() {
        int pos = dataBuffer.position();
        if (pos > 0)
            return pos;
        else
            return dataBuffer.limit();
    }

    @Override
    public NioBuffer slice(int start, int end) {
        DataBuffer newDataBuffer = dataBuffer.slice(start, end);
        return new NioBuffer(newDataBuffer);
    }

    @Override
    public NioBuffer getBuffer(int start, int end) {
        DataBuffer newDataBuffer = dataBuffer.getBuffer(start, end);
        return new NioBuffer(newDataBuffer);
    }

    @Override
    public short getUnsignedByte(int pos) {
        return dataBuffer.getUnsignedByte(pos);
    }

    @Override
    public NioBuffer appendByte(byte b) {
        dataBuffer.put(b);
        return this;
    }

    @Override
    public NioBuffer appendBytes(byte[] bytes, int offset, int len) {
        dataBuffer.put(bytes, offset, len);
        return this;
    }

    @Override
    public NioBuffer appendInt(int i) {
        dataBuffer.putInt(i);
        return this;
    }

    @Override
    public NioBuffer setByte(int pos, byte b) {
        dataBuffer.putByte(pos, b);
        return this;
    }
}
