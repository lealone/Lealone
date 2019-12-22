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

import java.io.IOException;
import java.io.InputStream;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.db.Constants;
import org.lealone.db.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLob;
import org.lealone.net.TransferOutputStream;
import org.lealone.server.TcpServerConnection;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.ReadLob;
import org.lealone.server.protocol.ReadLobAck;
import org.lealone.storage.LobStorage;

class ReadLobHandler implements PacketHandler<ReadLob> {
    @Override
    public Packet handle(TcpServerConnection conn, ServerSession session, ReadLob packet) {
        long lobId = packet.lobId;
        byte[] hmac = packet.hmac;
        long offset = packet.offset;
        int length = packet.length;
        SmallLRUCache<Long, CachedInputStream> lobs = conn.getLobs();
        CachedInputStream cachedInputStream = lobs.get(lobId);
        if (cachedInputStream == null) {
            cachedInputStream = new CachedInputStream(null);
            lobs.put(lobId, cachedInputStream);
        }
        try {
            TransferOutputStream.verifyLobMac(session, hmac, lobId);
            if (cachedInputStream.getPos() != offset) {
                LobStorage lobStorage = session.getDataHandler().getLobStorage();
                // only the lob id is used
                ValueLob lob = ValueLob.create(Value.BLOB, null, -1, lobId, hmac, -1);
                InputStream lobIn = lobStorage.getInputStream(lob, hmac, -1);
                cachedInputStream = new CachedInputStream(lobIn);
                lobs.put(lobId, cachedInputStream);
                lobIn.skip(offset);
            }
            // limit the buffer size
            length = Math.min(16 * Constants.IO_BUFFER_SIZE, length);
            byte[] buff = new byte[length];
            length = IOUtils.readFully(cachedInputStream, buff, length);
            if (length != buff.length) {
                byte[] newBuff = new byte[length];
                System.arraycopy(buff, 0, newBuff, 0, length);
                buff = newBuff;
            }
            return new ReadLobAck(buff);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }
}
