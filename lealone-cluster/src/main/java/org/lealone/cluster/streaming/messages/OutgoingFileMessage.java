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
package org.lealone.cluster.streaming.messages;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.lealone.cluster.dht.Range;
import org.lealone.cluster.dht.Token;
import org.lealone.cluster.io.DataOutputStreamPlus;
import org.lealone.cluster.streaming.StreamSession;
import org.lealone.cluster.streaming.StreamWriter;
import org.lealone.storage.StorageMap;

/**
 * OutgoingFileMessage is used to transfer the part(or whole) of a SSTable data file.
 */
public class OutgoingFileMessage extends StreamMessage {
    public static Serializer<OutgoingFileMessage> serializer = new Serializer<OutgoingFileMessage>() {
        @Override
        public OutgoingFileMessage deserialize(ReadableByteChannel in, int version, StreamSession session)
                throws IOException {
            throw new UnsupportedOperationException("Not allowed to call deserialize on an outgoing file");
        }

        @Override
        public void serialize(OutgoingFileMessage message, DataOutputStreamPlus out, int version, StreamSession session)
                throws IOException {
            message.serialize(out, version, session);
            session.fileSent(message.header);
        }
    };

    public final FileMessageHeader header;
    private final StorageMap<Object, Object> map;
    private final String filename;
    private boolean completed = false;

    public OutgoingFileMessage(StorageMap<Object, Object> map, int sequenceNumber, long estimatedKeys,
            List<Range<Token>> ranges) {
        super(Type.FILE);
        this.map = map;

        filename = map.getName();
        this.header = new FileMessageHeader(map.getName(), sequenceNumber, estimatedKeys, ranges);
    }

    public synchronized void serialize(DataOutputStreamPlus out, int version, StreamSession session) throws IOException {
        if (completed) {
            return;
        }

        FileMessageHeader.serializer.serialize(header, out, version);

        StreamWriter writer = new StreamWriter(map, null, null, session);
        writer.write(out.newDefaultChannel());
    }

    public synchronized void complete() {
        if (!completed) {
            completed = true;
        }
    }

    @Override
    public String toString() {
        return "File (" + header + ", file: " + filename + ")";
    }
}
