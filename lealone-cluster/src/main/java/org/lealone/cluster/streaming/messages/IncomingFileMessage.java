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
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.lealone.cluster.io.DataInputPlus;
import org.lealone.cluster.io.DataInputPlus.DataInputStreamPlus;
import org.lealone.cluster.io.DataOutputStreamPlus;
import org.lealone.cluster.streaming.StreamReader;
import org.lealone.cluster.streaming.StreamSession;
import org.lealone.cluster.utils.JVMStabilityInspector;

/**
 * IncomingFileMessage is used to receive the part(or whole) of a SSTable data file.
 */
public class IncomingFileMessage extends StreamMessage {
    public static Serializer<IncomingFileMessage> serializer = new Serializer<IncomingFileMessage>() {
        @Override
        @SuppressWarnings("resource")
        public IncomingFileMessage deserialize(ReadableByteChannel in, int version, StreamSession session)
                throws IOException {
            DataInputPlus input = new DataInputStreamPlus(Channels.newInputStream(in));
            FileMessageHeader header = FileMessageHeader.serializer.deserialize(input, version);
            StreamReader reader = new StreamReader(header, session, null);

            reader.read(in);

            try {
                return new IncomingFileMessage(header);
            } catch (Exception eof) {
                // Reading from remote failed(i.e. reached EOF before reading expected length of data).
                // This can be caused by network/node failure thus we are not retrying
                throw eof;
            } catch (Throwable t) {
                // Throwable can be Runtime error containing IOException.
                // In that case we don't want to retry.
                Throwable cause = t;
                while ((cause = cause.getCause()) != null) {
                    if (cause instanceof IOException)
                        throw (IOException) cause;
                }
                JVMStabilityInspector.inspectThrowable(t);
                // Otherwise, we can retry
                session.doRetry(header, t);
                return null;
            }
        }

        @Override
        public void serialize(IncomingFileMessage message, DataOutputStreamPlus out, int version, StreamSession session)
                throws IOException {
            throw new UnsupportedOperationException("Not allowed to call serialize on an incoming file");
        }
    };

    public FileMessageHeader header;

    public IncomingFileMessage(FileMessageHeader header) {
        super(Type.FILE);
        this.header = header;
    }

    @Override
    public String toString() {
        return "File (" + header + ", file: " + header.mapName + ")";
    }
}
