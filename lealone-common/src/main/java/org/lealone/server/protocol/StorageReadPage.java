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
package org.lealone.server.protocol;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.storage.PageKey;

public class StorageReadPage implements Packet {

    public final String mapName;
    public final PageKey pageKey;

    public StorageReadPage(String mapName, PageKey pageKey) {
        this.mapName = mapName;
        this.pageKey = pageKey;
    }

    @Override
    public PacketType getType() {
        return PacketType.COMMAND_STORAGE_READ_PAGE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.COMMAND_STORAGE_READ_PAGE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(mapName);
        out.writePageKey(pageKey);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageReadPage> {
        @Override
        public StorageReadPage decode(NetInputStream in, int version) throws IOException {
            String mapName = in.readString();
            PageKey pageKey = in.readPageKey();
            return new StorageReadPage(mapName, pageKey);
        }
    }
}
