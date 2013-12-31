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
package com.codefollower.lealone.atomicdb.streaming;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.UUID;

import com.codefollower.lealone.atomicdb.config.Schema;
import com.codefollower.lealone.atomicdb.db.ColumnFamilyStore;
import com.codefollower.lealone.atomicdb.db.DecoratedKey;
import com.codefollower.lealone.atomicdb.db.Directories;
import com.codefollower.lealone.atomicdb.db.Keyspace;
import com.codefollower.lealone.atomicdb.io.sstable.Component;
import com.codefollower.lealone.atomicdb.io.sstable.Descriptor;
import com.codefollower.lealone.atomicdb.io.sstable.SSTableReader;
import com.codefollower.lealone.atomicdb.io.sstable.SSTableWriter;
import com.codefollower.lealone.atomicdb.service.StorageService;
import com.codefollower.lealone.atomicdb.streaming.messages.FileMessageHeader;
import com.codefollower.lealone.atomicdb.utils.ByteBufferUtil;
import com.codefollower.lealone.atomicdb.utils.BytesReadTracker;
import com.codefollower.lealone.atomicdb.utils.Pair;
import com.google.common.base.Throwables;
import com.ning.compress.lzf.LZFInputStream;


/**
 * StreamReader reads from stream and writes to SSTable.
 */
public class StreamReader
{
    protected final UUID cfId;
    protected final long estimatedKeys;
    protected final Collection<Pair<Long, Long>> sections;
    protected final StreamSession session;
    protected final Descriptor.Version inputVersion;

    protected Descriptor desc;

    public StreamReader(FileMessageHeader header, StreamSession session)
    {
        this.session = session;
        this.cfId = header.cfId;
        this.estimatedKeys = header.estimatedKeys;
        this.sections = header.sections;
        this.inputVersion = new Descriptor.Version(header.version);
    }

    /**
     * @param channel where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    public SSTableReader read(ReadableByteChannel channel) throws IOException
    {
        long totalSize = totalSize();

        Pair<String, String> kscf = Schema.instance.getCF(cfId);
        ColumnFamilyStore cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        SSTableWriter writer = createWriter(cfs, totalSize);
        DataInputStream dis = new DataInputStream(new LZFInputStream(Channels.newInputStream(channel)));
        BytesReadTracker in = new BytesReadTracker(dis);
        try
        {
            while (in.getBytesRead() < totalSize)
            {
                writeRow(writer, in, cfs);
                // TODO move this to BytesReadTracker
                session.progress(desc, ProgressInfo.Direction.IN, in.getBytesRead(), totalSize);
            }
            return writer.closeAndOpenReader();
        }
        catch (Throwable e)
        {
            writer.abort();
            drain(dis, in.getBytesRead());
            if (e instanceof IOException)
                throw (IOException) e;
            else
                throw Throwables.propagate(e);
        }
    }

    protected SSTableWriter createWriter(ColumnFamilyStore cfs, long totalSize) throws IOException
    {
        Directories.DataDirectory localDir = cfs.directories.getWriteableLocation();
        if (localDir == null)
            throw new IOException("Insufficient disk space to store " + totalSize + " bytes");
        desc = Descriptor.fromFilename(cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(localDir)));

        return new SSTableWriter(desc.filenameFor(Component.DATA), estimatedKeys);
    }

    protected void drain(InputStream dis, long bytesRead) throws IOException
    {
        long toSkip = totalSize() - bytesRead;
        toSkip = toSkip - dis.skip(toSkip);
        while (toSkip > 0)
            toSkip = toSkip - dis.skip(toSkip);
    }

    protected long totalSize()
    {
        long size = 0;
        for (Pair<Long, Long> section : sections)
            size += section.right - section.left;
        return size;
    }

    protected void writeRow(SSTableWriter writer, DataInput in, ColumnFamilyStore cfs) throws IOException
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
        writer.appendFromStream(key, cfs.metadata, in, inputVersion);
        cfs.invalidateCachedRow(key);
    }
}
