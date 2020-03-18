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
package org.lealone.server.protocol.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.NoAckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class StorageReplicatePages implements NoAckPacket {

    public final String dbName;
    public final String storageName;
    public final ByteBuffer pages;

    public StorageReplicatePages(String dbName, String storageName, ByteBuffer pages) {
        this.dbName = dbName;
        this.storageName = storageName;
        this.pages = pages;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_REPLICATE_PAGES;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(dbName);
        out.writeString(storageName);
        out.writeByteBuffer(pages);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageReplicatePages> {
        @Override
        public StorageReplicatePages decode(NetInputStream in, int version) throws IOException {
            String dbName = in.readString();
            String storageName = in.readString();
            ByteBuffer pages = in.readByteBuffer();
            return new StorageReplicatePages(dbName, storageName, pages);
        }
    }
}
