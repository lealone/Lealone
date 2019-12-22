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
import java.util.ArrayList;
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.storage.PageKey;

public class CommandUpdate implements Packet {

    public final List<PageKey> pageKeys;
    public final String sql;

    public CommandUpdate(List<PageKey> pageKeys, String sql) {
        this.pageKeys = pageKeys;
        this.sql = sql;
    }

    @Override
    public PacketType getType() {
        return PacketType.COMMAND_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.COMMAND_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        if (pageKeys == null) {
            out.writeInt(0);
        } else {
            int size = pageKeys.size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                PageKey pk = pageKeys.get(i);
                out.writePageKey(pk);
            }
        }
        out.writeString(sql);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<CommandUpdate> {
        @Override
        public CommandUpdate decode(NetInputStream in, int version) throws IOException {
            List<PageKey> pageKeys = readPageKeys(in);
            String sql = in.readString();
            return new CommandUpdate(pageKeys, sql);
        }
    }

    protected static List<PageKey> readPageKeys(NetInputStream in) throws IOException {
        ArrayList<PageKey> pageKeys;
        int size = in.readInt();
        if (size > 0) {
            pageKeys = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                PageKey pk = in.readPageKey();
                pageKeys.add(pk);
            }
        } else {
            pageKeys = null;
        }
        return pageKeys;
    }
}
