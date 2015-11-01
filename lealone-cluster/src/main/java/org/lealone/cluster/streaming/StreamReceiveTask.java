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
package org.lealone.cluster.streaming;

/**
 * Task that manages receiving files for the session for certain ColumnFamily.
 */
public class StreamReceiveTask extends StreamTask {
    // private static final Logger logger = LoggerFactory.getLogger(StreamReceiveTask.class);

    // private static final ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory(
    // "StreamReceiveTask"));

    // number of files to receive
    private final int totalFiles;
    // total size of files to receive
    private final long totalSize;

    // true if task is done (either completed or aborted)
    private boolean done = false;

    public StreamReceiveTask(StreamSession session, String mapName, int totalFiles, long totalSize) {
        super(session, mapName);
        this.totalFiles = totalFiles;
        this.totalSize = totalSize;
    }

    /**
     * Process received file.
     */
    public synchronized void received(String mapName) {
        if (done)
            return;
    }

    @Override
    public int getTotalNumberOfFiles() {
        return totalFiles;
    }

    @Override
    public long getTotalSize() {
        return totalSize;
    }

    /**
     * Abort this task.
     * If the task already received all files and
     * {@link org.lealone.cluster.streaming.StreamReceiveTask.OnCompletionRunnable} task is submitted,
     * then task cannot be aborted.
     */
    @Override
    public synchronized void abort() {
        if (done)
            return;

        done = true;
    }
}
