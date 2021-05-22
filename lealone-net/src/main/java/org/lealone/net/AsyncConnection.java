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
package org.lealone.net;

import java.net.InetSocketAddress;

/**
 * An async connection.
 */
public abstract class AsyncConnection {

    protected final WritableChannel writableChannel;
    protected final boolean isServer;
    protected InetSocketAddress inetSocketAddress;
    protected boolean closed;

    public AsyncConnection(WritableChannel writableChannel, boolean isServer) {
        this.writableChannel = writableChannel;
        this.isServer = isServer;
    }

    public abstract void handle(NetBuffer buffer);

    public WritableChannel getWritableChannel() {
        return writableChannel;
    }

    public InetSocketAddress getInetSocketAddress() {
        return inetSocketAddress;
    }

    public void setInetSocketAddress(InetSocketAddress inetSocketAddress) {
        this.inetSocketAddress = inetSocketAddress;
    }

    public void close() {
        closed = true;
        if (writableChannel != null) {
            writableChannel.close();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public void checkClosed() {
        if (closed) {
            throw new RuntimeException("Connection[" + inetSocketAddress.getHostName() + "] is closed");
        }
    }

    public void handleException(Exception e) {
    }
}
