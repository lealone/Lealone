/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.lealone.server.http.util.net;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.server.http.util.ExceptionUtils;
import com.lealone.server.http.util.collections.SynchronizedStack;
import com.lealone.server.http.util.compat.JrePlatform;

/**
 * NIO endpoint.
 */
public class NioEndpoint extends AbstractNetworkChannelEndpoint<NioChannel, SocketChannel> {

    // -------------------------------------------------------------- Constants

    private static final Logger log = LoggerFactory.getLogger(NioEndpoint.class);
    private static final Logger logCertificate = LoggerFactory
            .getLogger(NioEndpoint.class.getName() + ".certificate");

    // ----------------------------------------------------------------- Fields

    /**
     * Server socket "pointer".
     */
    private volatile ServerSocketChannel serverSock = null;

    /**
     * Cache for poller events
     */
    private SynchronizedStack<PollerEvent> eventCache;

    /**
     * Bytebuffer cache, each channel holds a set of buffers (two, except for SSL holds four)
     */
    private SynchronizedStack<NioChannel> nioChannels;

    private SocketAddress previousAcceptedSocketRemoteAddress = null;
    private long previousAcceptedSocketNanoTime = 0;

    // ------------------------------------------------------------- Properties

    /**
     * Use System.inheritableChannel to obtain channel from stdin/stdout.
     */
    private boolean useInheritedChannel = false;

    public void setUseInheritedChannel(boolean useInheritedChannel) {
        this.useInheritedChannel = useInheritedChannel;
    }

    public boolean getUseInheritedChannel() {
        return useInheritedChannel;
    }

    /**
     * Path for the Unix domain socket, used to create the socket address.
     */
    private String unixDomainSocketPath = null;

    public String getUnixDomainSocketPath() {
        return this.unixDomainSocketPath;
    }

    public void setUnixDomainSocketPath(String unixDomainSocketPath) {
        this.unixDomainSocketPath = unixDomainSocketPath;
    }

    /**
     * Permissions which will be set on the Unix domain socket if it is created.
     */
    private String unixDomainSocketPathPermissions = null;

    public String getUnixDomainSocketPathPermissions() {
        return this.unixDomainSocketPathPermissions;
    }

    public void setUnixDomainSocketPathPermissions(String unixDomainSocketPathPermissions) {
        this.unixDomainSocketPathPermissions = unixDomainSocketPathPermissions;
    }

    private long selectorTimeout = 1000;

    public void setSelectorTimeout(long timeout) {
        this.selectorTimeout = timeout;
    }

    public long getSelectorTimeout() {
        return this.selectorTimeout;
    }

    // --------------------------------------------------------- Public Methods

    @Override
    public String getId() {
        if (getUseInheritedChannel()) {
            return "JVMInheritedChannel";
        } else if (getUnixDomainSocketPath() != null) {
            return getUnixDomainSocketPath();
        } else {
            return null;
        }
    }

    // ----------------------------------------------- Public Lifecycle Methods

    /**
     * Initialize the endpoint.
     */
    @Override
    public void bind() throws Exception {
        // initServerSocket();

        // Initialize SSL if needed
        initialiseSsl();
    }

    // Separated out to make it easier for folks that extend NioEndpoint to
    // implement custom [server]sockets
    protected void initServerSocket() throws Exception {
        if (getUseInheritedChannel()) {
            // Retrieve the channel provided by the OS
            Channel ic = System.inheritedChannel();
            if (ic instanceof ServerSocketChannel) {
                serverSock = (ServerSocketChannel) ic;
            }
            if (serverSock == null) {
                throw new IllegalArgumentException(sm.getString("endpoint.init.bind.inherited"));
            }
        } else if (getUnixDomainSocketPath() != null) {
            SocketAddress sa = UnixDomainSocketAddress.of(getUnixDomainSocketPath());
            serverSock = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            serverSock.bind(sa, getAcceptCount());
            if (getUnixDomainSocketPathPermissions() != null) {
                Path path = Paths.get(getUnixDomainSocketPath());
                Set<PosixFilePermission> permissions = PosixFilePermissions
                        .fromString(getUnixDomainSocketPathPermissions());
                if (path.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                    FileAttribute<Set<PosixFilePermission>> attrs = PosixFilePermissions
                            .asFileAttribute(permissions);
                    Files.setAttribute(path, attrs.name(), attrs.value());
                } else {
                    File file = path.toFile();
                    if (permissions.contains(PosixFilePermission.OTHERS_READ)
                            && !file.setReadable(true, false)) {
                        log.warn(sm.getString("endpoint.nio.perms.readFail", file.getPath()));
                    }
                    if (permissions.contains(PosixFilePermission.OTHERS_WRITE)
                            && !file.setWritable(true, false)) {
                        log.warn(sm.getString("endpoint.nio.perms.writeFail", file.getPath()));
                    }
                }
            }
        } else {
            serverSock = ServerSocketChannel.open();
            socketProperties.setProperties(serverSock.socket());
            InetSocketAddress addr = new InetSocketAddress(getAddress(), getPortWithOffset());
            serverSock.bind(addr, getAcceptCount());
        }
        serverSock.configureBlocking(true); // mimic APR behavior
    }

    /**
     * Start the NIO endpoint, creating acceptor, poller threads.
     */
    @Override
    public void startInternal() throws Exception {

        if (!running) {
            running = true;
            paused = false;

            if (socketProperties.getEventCache() != 0) {
                eventCache = new SynchronizedStack<>(SynchronizedStack.DEFAULT_SIZE,
                        socketProperties.getEventCache());
            }
            int actualBufferPool = socketProperties
                    .getActualBufferPool(isSSLEnabled() ? getSniParseLimit() * 2 : 0);
            if (actualBufferPool != 0) {
                nioChannels = new SynchronizedStack<>(SynchronizedStack.DEFAULT_SIZE, actualBufferPool);
            }
        }
    }

    /**
     * Stop the endpoint. This will cause all processing threads to stop.
     */
    @Override
    public void stopInternal() {
        if (!paused) {
            pause();
        }
        if (running) {
            running = false;
            if (eventCache != null) {
                eventCache.clear();
                eventCache = null;
            }
            if (nioChannels != null) {
                NioChannel socket;
                while ((socket = nioChannels.pop()) != null) {
                    socket.free();
                }
                nioChannels = null;
            }
        }
    }

    /**
     * Deallocate NIO memory pools, and close server socket.
     */
    @Override
    public void unbind() throws Exception {
        if (log.isTraceEnabled()) {
            log.trace(
                    "Destroy initiated for " + new InetSocketAddress(getAddress(), getPortWithOffset()));
        }
        if (running) {
            stop();
        }
        try {
            doCloseServerSocket();
        } catch (IOException ioe) {
            getLog().warn(sm.getString("endpoint.serverSocket.closeFailed", getName()), ioe);
        }
        destroySsl();
        super.unbind();
        if (getHandler() != null) {
            getHandler().recycle();
        }
        if (log.isTraceEnabled()) {
            log.trace(
                    "Destroy completed for " + new InetSocketAddress(getAddress(), getPortWithOffset()));
        }
    }

    @Override
    protected void doCloseServerSocket() throws IOException {
        try {
            if (!getUseInheritedChannel() && serverSock != null) {
                // Close server socket
                serverSock.close();
            }
            serverSock = null;
        } finally {
            if (getUnixDomainSocketPath() != null && getBindState().wasBound()) {
                Files.delete(Paths.get(getUnixDomainSocketPath()));
            }
        }
    }

    // ------------------------------------------------------ Protected Methods

    protected SynchronizedStack<NioChannel> getNioChannels() {
        return nioChannels;
    }

    public NioSocketWrapper createSocketWrapper(SocketChannel socket) {
        setSocketOptions(socket);
        return (NioSocketWrapper) connections.get(socket);
    }

    /**
     * Process the specified connection.
     *
     * @param socket The socket channel
     *
     * @return <code>true</code> if the socket was correctly configured and processing may continue, <code>false</code>
     *             if the socket needs to be close immediately
     */
    @Override
    protected boolean setSocketOptions(SocketChannel socket) {
        NioSocketWrapper socketWrapper = null;
        try {
            // Allocate channel and wrapper
            NioChannel channel = null;
            if (nioChannels != null) {
                channel = nioChannels.pop();
            }
            if (channel == null) {
                SocketBufferHandler bufhandler = new SocketBufferHandler(
                        socketProperties.getAppReadBufSize(), socketProperties.getAppWriteBufSize(),
                        socketProperties.getDirectBuffer());
                channel = createChannel(bufhandler);
            }
            NioSocketWrapper newWrapper = new NioSocketWrapper(channel, this);
            channel.reset(socket, newWrapper);
            connections.put(socket, newWrapper);
            socketWrapper = newWrapper;

            // Set socket properties
            // Disable blocking, polling will be used
            socket.configureBlocking(false);
            if (getUnixDomainSocketPath() == null) {
                socketProperties.setProperties(socket.socket());
            }

            socketWrapper.setReadTimeout(getConnectionTimeout());
            socketWrapper.setWriteTimeout(getConnectionTimeout());
            socketWrapper.setKeepAliveLeft(NioEndpoint.this.getMaxKeepAliveRequests());
            return true;
        } catch (Throwable t) {
            ExceptionUtils.handleThrowable(t);
            try {
                log.error(sm.getString("endpoint.socketOptionsError"), t);
            } catch (Throwable tt) {
                ExceptionUtils.handleThrowable(tt);
            }
            if (socketWrapper == null) {
                destroySocket(socket);
            }
        }
        // Tell to close the socket if needed
        return false;
    }

    @Override
    protected void destroySocket(SocketChannel socket) {
        try {
            socket.close();
        } catch (IOException ioe) {
            if (log.isDebugEnabled()) {
                log.debug(sm.getString("endpoint.err.close"), ioe);
            }
        }
    }

    @Override
    protected NetworkChannel getServerSocket() {
        return serverSock;
    }

    @Override
    protected SocketChannel serverSocketAccept() throws Exception {
        SocketChannel result = serverSock.accept();

        // Bug does not affect Windows platform and Unix Domain Socket. Skip the check.
        if (!JrePlatform.IS_WINDOWS && getUnixDomainSocketPath() == null) {
            SocketAddress currentRemoteAddress = result.getRemoteAddress();
            long currentNanoTime = System.nanoTime();
            if (currentRemoteAddress.equals(previousAcceptedSocketRemoteAddress)
                    && currentNanoTime - previousAcceptedSocketNanoTime < 1000) {
                throw new IOException(sm.getString("endpoint.err.duplicateAccept"));
            }
            previousAcceptedSocketRemoteAddress = currentRemoteAddress;
            previousAcceptedSocketNanoTime = currentNanoTime;
        }

        return result;
    }

    @Override
    protected Logger getLog() {
        return log;
    }

    @Override
    protected Logger getLogCertificate() {
        return logCertificate;
    }

    @Override
    protected NioChannel createChannel(SocketBufferHandler buffer) {
        if (isSSLEnabled()) {
            return new SecureNioChannel(buffer, this);
        }
        return new NioChannel(buffer);
    }

    // ----------------------------------------------------- Poller Inner Classes

    /**
     * PollerEvent, cacheable object for poller events to avoid GC
     */
    public static class PollerEvent {

        private NioSocketWrapper socketWrapper;
        private int interestOps;

        public PollerEvent(NioSocketWrapper socketWrapper, int intOps) {
            reset(socketWrapper, intOps);
        }

        public void reset(NioSocketWrapper socketWrapper, int intOps) {
            this.socketWrapper = socketWrapper;
            interestOps = intOps;
        }

        public NioSocketWrapper getSocketWrapper() {
            return socketWrapper;
        }

        public int getInterestOps() {
            return interestOps;
        }

        public void reset() {
            reset(null, 0);
        }

        @Override
        public String toString() {
            return "Poller event: socket [" + socketWrapper.getSocket() + "], socketWrapper ["
                    + socketWrapper + "], interestOps [" + interestOps + "]";
        }
    }

    // ----------------------------------------------- SendfileData Inner Class

    /**
     * SendfileData class.
     */
    public static class SendfileData extends SendfileDataBase {

        public SendfileData(String filename, long pos, long length) {
            super(filename, pos, length);
        }

        protected volatile FileChannel fchannel;
    }
}
