/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lealone.server.http.util.net;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ConcurrentModificationException;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.server.http.util.ExceptionUtils;
import com.lealone.server.http.util.collections.SynchronizedStack;
import com.lealone.server.http.util.net.AbstractEndpoint.Handler.SocketState;
import com.lealone.server.http.util.net.NioEndpoint.SendfileData;
import com.lealone.server.http.util.net.jsse.JSSESupport;

public class NioSocketWrapper extends SocketWrapper<NioChannel> {

    private static final Logger log = LoggerFactory.getLogger(NioSocketWrapper.class);

    private static final Logger logHandshake = LoggerFactory
            .getLogger(NioSocketWrapper.class.getName() + ".handshake");

    private final SynchronizedStack<NioChannel> nioChannels;

    private int interestOps = 0;
    private volatile SendfileData sendfileData = null;
    private volatile long lastRead = System.currentTimeMillis();
    private volatile long lastWrite = lastRead;

    final Object readLock;
    volatile boolean readBlocking = false;
    final Object writeLock;
    volatile boolean writeBlocking = false;

    public NioSocketWrapper(NioChannel channel, NioEndpoint endpoint) {
        super(channel, endpoint);
        if (endpoint.getUnixDomainSocketPath() != null) {
            // Pretend localhost for easy compatibility
            localAddr = "127.0.0.1";
            localName = "localhost";
            localPort = 0;
            remoteAddr = "127.0.0.1";
            remoteHost = "localhost";
            remotePort = 0;
        }
        nioChannels = endpoint.getNioChannels();
        socketBufferHandler = channel.getBufHandler();
        readLock = (readPending == null) ? new Object() : readPending;
        writeLock = (writePending == null) ? new Object() : writePending;
    }

    public int interestOps() {
        return interestOps;
    }

    public int interestOps(int ops) {
        this.interestOps = ops;
        return ops;
    }

    public boolean interestOpsHas(int targetOp) {
        return (this.interestOps() & targetOp) == targetOp;
    }

    public void setSendfileData(SendfileData sf) {
        this.sendfileData = sf;
    }

    public SendfileData getSendfileData() {
        return this.sendfileData;
    }

    public void updateLastWrite() {
        lastWrite = System.currentTimeMillis();
    }

    public long getLastWrite() {
        return lastWrite;
    }

    public void updateLastRead() {
        lastRead = System.currentTimeMillis();
    }

    public long getLastRead() {
        return lastRead;
    }

    @Override
    public boolean isReadyForRead() throws IOException {
        socketBufferHandler.configureReadBufferForRead();

        if (socketBufferHandler.getReadBuffer().remaining() > 0) {
            return true;
        }

        fillReadBuffer(false);

        return socketBufferHandler.getReadBuffer().position() > 0;
    }

    @Override
    public int read(boolean block, byte[] b, int off, int len) throws IOException {
        int nRead = populateReadBuffer(b, off, len);
        if (nRead > 0) {
            return nRead;
            /*
             * Since more bytes may have arrived since the buffer was last filled, it is an option at this point to
             * perform a non-blocking read. However, correctly handling the case if that read returns end of stream
             * adds complexity. Therefore, at the moment, the preference is for simplicity.
             */
        }

        // Fill the read buffer as best we can.
        nRead = fillReadBuffer(block);
        updateLastRead();

        // Fill as much of the remaining byte array as possible with the
        // data that was just read
        if (nRead > 0) {
            socketBufferHandler.configureReadBufferForRead();
            nRead = Math.min(nRead, len);
            socketBufferHandler.getReadBuffer().get(b, off, nRead);
        }
        return nRead;
    }

    @Override
    public int read(boolean block, ByteBuffer to) throws IOException {
        int nRead = populateReadBuffer(to);
        if (nRead > 0) {
            return nRead;
            /*
             * Since more bytes may have arrived since the buffer was last filled, it is an option at this point to
             * perform a non-blocking read. However, correctly handling the case if that read returns end of stream
             * adds complexity. Therefore, at the moment, the preference is for simplicity.
             */
        }

        // The socket read buffer capacity is socket.appReadBufSize
        int limit = socketBufferHandler.getReadBuffer().capacity();
        if (to.remaining() >= limit) {
            to.limit(to.position() + limit);
            nRead = fillReadBuffer(block, to);
            if (log.isTraceEnabled()) {
                log.trace("Socket: [" + this + "], Read direct from socket: [" + nRead + "]");
            }
            updateLastRead();
        } else {
            // Fill the read buffer as best we can.
            nRead = fillReadBuffer(block);
            if (log.isTraceEnabled()) {
                log.trace("Socket: [" + this + "], Read into buffer: [" + nRead + "]");
            }
            updateLastRead();

            // Fill as much of the remaining byte array as possible with the
            // data that was just read
            if (nRead > 0) {
                nRead = populateReadBuffer(to);
            }
        }
        return nRead;
    }

    @Override
    protected void doClose() {
        if (log.isTraceEnabled()) {
            log.trace("Calling [" + getEndpoint() + "].closeSocket([" + this + "])");
        }
        try {
            getEndpoint().connections.remove(getSocket().getIOChannel());
            if (getSocket().isOpen()) {
                getSocket().close(true);
            }
            if (getEndpoint().running) {
                getSocket().reset(null, null);
                if (nioChannels == null || !nioChannels.push(getSocket())) {
                    getSocket().free();
                }
            }
        } catch (Throwable t) {
            ExceptionUtils.handleThrowable(t);
            if (log.isDebugEnabled()) {
                log.error(sm.getString("endpoint.debug.channelCloseFail"), t);
            }
        } finally {
            socketBufferHandler = SocketBufferHandler.EMPTY;
            nonBlockingWriteBuffer.clear();
            reset(NioChannel.CLOSED_NIO_CHANNEL);
        }
        try {
            SendfileData data = getSendfileData();
            if (data != null && data.fchannel != null && data.fchannel.isOpen()) {
                data.fchannel.close();
            }
        } catch (Throwable t) {
            ExceptionUtils.handleThrowable(t);
            if (log.isDebugEnabled()) {
                log.error(sm.getString("endpoint.sendfile.closeError"), t);
            }
        }
    }

    private int fillReadBuffer(boolean block) throws IOException {
        socketBufferHandler.configureReadBufferForWrite();
        return fillReadBuffer(block, socketBufferHandler.getReadBuffer());
    }

    private int fillReadBuffer(boolean block, ByteBuffer buffer) throws IOException {
        int n;
        if (getSocket() == NioChannel.CLOSED_NIO_CHANNEL) {
            throw new ClosedChannelException();
        }
        if (block) {
            long timeout = getReadTimeout();
            long startNanos = 0;
            do {
                if (startNanos > 0) {
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    if (elapsedMillis == 0) {
                        elapsedMillis = 1;
                    }
                    timeout -= elapsedMillis;
                    if (timeout <= 0) {
                        throw new SocketTimeoutException();
                    }
                }
                synchronized (readLock) {
                    n = getSocket().read(buffer);
                    if (n == -1) {
                        throw new EOFException();
                    } else if (n == 0) {
                        // Ensure a spurious wake-up doesn't trigger a duplicate registration
                        if (!readBlocking) {
                            readBlocking = true;
                            registerReadInterest();
                        }
                        try {
                            if (timeout > 0) {
                                startNanos = System.nanoTime();
                                readLock.wait(timeout);
                            } else {
                                readLock.wait();
                            }
                        } catch (InterruptedException ignore) {
                            /*
                             * Most likely the Poller signalling there is data to read but could be spurious. Exit
                             * the wait, check status and proceed accordingly.
                             */
                        }
                    }
                }
            } while (n == 0); // TLS needs to loop as reading zero application bytes is possible
        } else {
            n = getSocket().read(buffer);
            if (n == -1) {
                throw new EOFException();
            }
        }
        return n;
    }

    @Override
    protected boolean flushNonBlocking() throws IOException {
        boolean dataLeft = socketOrNetworkBufferHasDataLeft();

        // Write to the socket, if there is anything to write
        if (dataLeft) {
            doWrite(false);
            dataLeft = socketOrNetworkBufferHasDataLeft();
        }

        if (!dataLeft && !nonBlockingWriteBuffer.isEmpty()) {
            dataLeft = nonBlockingWriteBuffer.write(this, false);

            if (!dataLeft && socketOrNetworkBufferHasDataLeft()) {
                doWrite(false);
                dataLeft = socketOrNetworkBufferHasDataLeft();
            }
        }

        return dataLeft;
    }

    /*
     * https://bz.apache.org/bugzilla/show_bug.cgi?id=66076
     *
     * When using TLS an additional buffer is used for the encrypted data before it is written to the network. It is
     * possible for this network output buffer to contain data while the socket write buffer is empty.
     *
     * For NIO with non-blocking I/O, this case is handling by ensuring that flush only returns false (i.e. no data
     * left to flush) if all buffers are empty.
     */
    private boolean socketOrNetworkBufferHasDataLeft() {
        return !socketBufferHandler.isWriteBufferEmpty() || getSocket().getOutboundRemaining() > 0;
    }

    /*
     * https://bz.apache.org/bugzilla/show_bug.cgi?id=69982
     *
     * Similar to socketOrNetworkBufferHasDataLeft(), check the additional buffer for TLS.
     */
    @Override
    public boolean hasDataToWrite() {
        return super.hasDataToWrite() || getSocket().getOutboundRemaining() > 0;
    }

    /*
     * https://bz.apache.org/bugzilla/show_bug.cgi?id=69982
     *
     * Similar to socketOrNetworkBufferHasDataLeft(), check the additional buffer for TLS.
     */
    @Override
    public boolean canWrite() {
        return super.canWrite() && getSocket().getOutboundRemaining() == 0;
    }

    @Override
    protected void doWrite(boolean block, ByteBuffer buffer) throws IOException {
        int n;
        if (getSocket() == NioChannel.CLOSED_NIO_CHANNEL) {
            throw new ClosedChannelException();
        }
        if (block) {
            if (previousIOException != null) {
                /*
                 * Socket has previously timed out.
                 *
                 * Blocking writes assume that buffer is always fully written so there is no code checking for
                 * incomplete writes, retaining the unwritten data and attempting to write it as part of a
                 * subsequent write call.
                 *
                 * Because of the above, when a timeout is triggered we need to skip subsequent attempts to write as
                 * otherwise it will appear to the client as if some data was dropped just before the connection is
                 * lost. It is better if the client just sees the dropped connection.
                 */
                throw new IOException(previousIOException);
            }
            long timeout = getWriteTimeout();
            long startNanos = 0;
            do {
                if (startNanos > 0) {
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    if (elapsedMillis == 0) {
                        elapsedMillis = 1;
                    }
                    timeout -= elapsedMillis;
                    if (timeout <= 0) {
                        previousIOException = new SocketTimeoutException();
                        throw previousIOException;
                    }
                }
                synchronized (writeLock) {
                    n = getSocket().write(buffer);
                    // n == 0 could be an incomplete write, but it could also
                    // indicate that a previous incomplete write of the
                    // outbound buffer (for TLS) has now completed. Only
                    // block if there is still data to write.
                    if (n == 0 && (buffer.hasRemaining() || getSocket().getOutboundRemaining() > 0)) {
                        // Ensure a spurious wake-up doesn't trigger a duplicate registration
                        if (!writeBlocking) {
                            writeBlocking = true;
                            registerWriteInterest();
                        }
                        try {
                            if (timeout > 0) {
                                startNanos = System.nanoTime();
                                writeLock.wait(timeout);
                            } else {
                                writeLock.wait();
                            }
                        } catch (InterruptedException ignore) {
                            /*
                             * Most likely the Poller signalling that data can be written but could be spurious.
                             * Exit the wait, check status and proceed accordingly.
                             */
                        }
                    } else if (startNanos > 0) {
                        // If something was written, reset timeout
                        timeout = getWriteTimeout();
                        startNanos = 0;
                    }
                }
            } while (buffer.hasRemaining() || getSocket().getOutboundRemaining() > 0);
        } else {
            do {
                n = getSocket().write(buffer);
            } while (n > 0 && buffer.hasRemaining());
            // If there is data left in the buffer the socket will be registered for
            // write further up the stack. This is to ensure the socket is only
            // registered for write once as both container and user code can trigger
            // write registration.
        }
        updateLastWrite();
    }

    @Override
    public void registerReadInterest() {
        if (log.isTraceEnabled()) {
            log.trace(sm.getString("endpoint.debug.registerRead", this));
        }
        add(this, SelectionKey.OP_READ);
    }

    @Override
    public void registerWriteInterest() {
        if (log.isTraceEnabled()) {
            log.trace(sm.getString("endpoint.debug.registerWrite", this));
        }
        add(this, SelectionKey.OP_WRITE);
    }

    private void add(NioSocketWrapper socketWrapper, int interestOps) {
        if (socketWrapper.getSocket() instanceof SecureNioChannel channel) {
            if (channel.isHandshakeComplete())
                return;
            if (interestOps == SelectionKey.OP_WRITE)
                socketWrapper.processSocket(SocketEvent.OPEN_WRITE, false);
            else if (interestOps == SelectionKey.OP_READ)
                socketWrapper.processSocket(SocketEvent.OPEN_READ, false);
        } else {
            if (isClosed()) {
                socketWrapper.processSocket(SocketEvent.STOP, false);
            }
        }
    }

    @Override
    public SendfileDataBase createSendfileData(String filename, long pos, long length) {
        return new SendfileData(filename, pos, length);
    }

    @Override
    public SendfileState processSendfile(SendfileDataBase sendfileData) {
        setSendfileData((SendfileData) sendfileData);
        SelectionKey key = getSocket().getIOChannel().keyFor(getSelector());
        if (key == null) {
            return SendfileState.ERROR;
        } else {
            // Might as well do the first write on this thread
            return processSendfile(key, this, true);
        }
    }

    @Override
    protected void populateRemoteAddr() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            InetAddress inetAddr = sc.socket().getInetAddress();
            if (inetAddr != null) {
                remoteAddr = inetAddr.getHostAddress();
            }
        }
    }

    @Override
    protected void populateRemoteHost() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            InetAddress inetAddr = sc.socket().getInetAddress();
            if (inetAddr != null) {
                remoteHost = inetAddr.getHostName();
                if (remoteAddr == null) {
                    remoteAddr = inetAddr.getHostAddress();
                }
            }
        }
    }

    @Override
    protected void populateRemotePort() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            remotePort = sc.socket().getPort();
        }
    }

    @Override
    protected void populateLocalName() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            InetAddress inetAddr = sc.socket().getLocalAddress();
            if (inetAddr != null) {
                localName = inetAddr.getHostName();
            }
        }
    }

    @Override
    protected void populateLocalAddr() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            InetAddress inetAddr = sc.socket().getLocalAddress();
            if (inetAddr != null) {
                localAddr = inetAddr.getHostAddress();
            }
        }
    }

    @Override
    protected void populateLocalPort() {
        SocketChannel sc = getSocket().getIOChannel();
        if (sc != null) {
            localPort = sc.socket().getLocalPort();
        }
    }

    @Override
    public SSLSupport getSslSupport() {
        if (getSocket() instanceof SecureNioChannel ch) {
            return ch.getSSLSupport();
        }
        return null;
    }

    @Override
    public void doClientAuth(SSLSupport sslSupport) throws IOException {
        SecureNioChannel sslChannel = (SecureNioChannel) getSocket();
        SSLEngine engine = sslChannel.getSslEngine();
        if (!engine.getNeedClientAuth()) {
            // Need to re-negotiate SSL connection
            engine.setNeedClientAuth(true);
            sslChannel.rehandshake(getEndpoint().getConnectionTimeout());
            ((JSSESupport) sslSupport).setSession(engine.getSession());
        }
    }

    @Override
    public void setAppReadBufHandler(ApplicationBufferHandler handler) {
        getSocket().setAppReadBufHandler(handler);
    }

    @Override
    protected <A> OperationState<A> newOperationState(boolean read, ByteBuffer[] buffers, int offset,
            int length, BlockingMode block, long timeout, TimeUnit unit, A attachment,
            CompletionCheck check, CompletionHandler<Long, ? super A> handler, Semaphore semaphore,
            VectoredIOCompletionHandler<A> completion) {
        return new NioOperationState<>(read, buffers, offset, length, block, timeout, unit, attachment,
                check, handler, semaphore, completion);
    }

    private class NioOperationState<A> extends OperationState<A> {
        private volatile boolean inline = true;

        private NioOperationState(boolean read, ByteBuffer[] buffers, int offset, int length,
                BlockingMode block, long timeout, TimeUnit unit, A attachment, CompletionCheck check,
                CompletionHandler<Long, ? super A> handler, Semaphore semaphore,
                VectoredIOCompletionHandler<A> completion) {
            super(read, buffers, offset, length, block, timeout, unit, attachment, check, handler,
                    semaphore, completion);
        }

        @Override
        protected boolean isInline() {
            return inline;
        }

        @Override
        protected boolean hasOutboundRemaining() {
            return getSocket().getOutboundRemaining() > 0;
        }

        @Override
        public void run() {
            // if (getInputBuffer() != null) {
            // handler.completed(null, null);
            // return;
            // }
            // Perform the IO operation
            // Called from the poller to continue the IO operation
            long nBytes = 0;
            if (getError() == null) {
                try {
                    synchronized (this) {
                        if (!completionDone) {
                            // This filters out same notification until processing
                            // of the current one is done
                            if (log.isTraceEnabled()) {
                                log.trace("Skip concurrent " + (read ? "read" : "write")
                                        + " notification");
                            }
                            return;
                        }
                        if (read) {
                            // Read from main buffer first
                            if (!socketBufferHandler.isReadBufferEmpty()) {
                                // There is still data inside the main read buffer, it needs to be read first
                                socketBufferHandler.configureReadBufferForRead();
                                for (int i = 0; i < length
                                        && !socketBufferHandler.isReadBufferEmpty(); i++) {
                                    nBytes += transfer(socketBufferHandler.getReadBuffer(),
                                            buffers[offset + i]);
                                }
                            }
                            if (nBytes == 0) {
                                nBytes = getSocket().read(buffers, offset, length);
                                updateLastRead();
                            }
                        } else {
                            boolean doWrite = true;
                            // Write from main buffer first
                            if (socketOrNetworkBufferHasDataLeft()) {
                                // There is still data inside the main write buffer, it needs to be written first
                                socketBufferHandler.configureWriteBufferForRead();
                                do {
                                    nBytes = getSocket().write(socketBufferHandler.getWriteBuffer());
                                } while (socketOrNetworkBufferHasDataLeft() && nBytes > 0);
                                if (socketOrNetworkBufferHasDataLeft()) {
                                    doWrite = false;
                                }
                                // Preserve a negative value since it is an error
                                if (nBytes > 0) {
                                    nBytes = 0;
                                }
                            }
                            if (doWrite) {
                                // 先算出总数，避免多次调用write
                                long total = 0;
                                for (int i = 0, len = buffers.length; i < len; i++)
                                    total += buffers[i].remaining();
                                long n;
                                do {
                                    n = getSocket().write(buffers, offset, length);
                                    if (n == -1) {
                                        nBytes = n;
                                    } else {
                                        nBytes += n;
                                    }
                                } while (n > 0 && nBytes < total);
                                updateLastWrite();
                            }
                        }
                        if (nBytes != 0 || (!buffersArrayHasRemaining(buffers, offset, length)
                                && (read || !socketOrNetworkBufferHasDataLeft()))) {
                            completionDone = false;
                        }
                    }
                } catch (IOException ioe) {
                    setError(ioe);
                }
            }
            if (nBytes > 0 || (nBytes == 0 && !buffersArrayHasRemaining(buffers, offset, length)
                    && (read || !socketOrNetworkBufferHasDataLeft()))) {
                // The bytes processed are only updated in the completion handler
                completion.completed(Long.valueOf(nBytes), this);
            } else if (nBytes < 0 || getError() != null) {
                IOException error = getError();
                if (error == null) {
                    error = new EOFException();
                }
                completion.failed(error, this);
            } else {
                // As soon as the operation uses the poller, it is no longer inline
                inline = false;
                if (read) {
                    registerReadInterest();
                } else {
                    registerWriteInterest();
                }
            }
        }
    }

    @Override
    protected boolean processSocketInternal(SocketEvent event) {
        try {
            int handshake;
            try {
                if (getSocket().isHandshakeComplete()) {
                    // No TLS handshaking required. Let the handler
                    // process this socket / event combination.
                    handshake = 0;
                } else if (event == SocketEvent.STOP || event == SocketEvent.DISCONNECT
                        || event == SocketEvent.ERROR) {
                    // Unable to complete the TLS handshake. Treat it as
                    // if the handshake failed.
                    handshake = -1;
                } else {
                    handshake = getSocket().handshake(event == SocketEvent.OPEN_READ,
                            event == SocketEvent.OPEN_WRITE);
                    // The handshake process reads/writes from/to the
                    // socket. status may therefore be OPEN_WRITE once
                    // the handshake completes. However, the handshake
                    // happens when the socket is opened so the status
                    // must always be OPEN_READ after it completes. It
                    // is OK to always set this as it is only used if
                    // the handshake completes.
                    event = SocketEvent.OPEN_READ;
                }
            } catch (IOException ioe) {
                handshake = -1;
                if (logHandshake.isDebugEnabled()) {
                    logHandshake.debug(sm.getString("endpoint.err.handshake", getRemoteAddr(),
                            Integer.toString(getRemotePort())), ioe);
                }
            } catch (CancelledKeyException ckx) {
                handshake = -1;
            }
            if (handshake == 0) {
                SocketState state;
                // Process the request from this socket
                state = getEndpoint().getHandler().process(this,
                        Objects.requireNonNullElse(event, SocketEvent.OPEN_READ));
                if (state == SocketState.CLOSED) {
                    close();
                }
            } else if (handshake == -1) {
                getEndpoint().getHandler().process(this, SocketEvent.CONNECT_FAIL);
                close();
            } else if (handshake == SelectionKey.OP_READ) {
                registerReadInterest();
            } else if (handshake == SelectionKey.OP_WRITE) {
                registerWriteInterest();
            }
        } catch (CancelledKeyException cx) {
            close();
        } catch (VirtualMachineError vme) {
            ExceptionUtils.handleThrowable(vme);
        } catch (Throwable t) {
            log.error(sm.getString("endpoint.processing.fail"), t);
            close();
        }
        return true;
    }

    public SendfileState processSendfile(SelectionKey sk, NioSocketWrapper socketWrapper,
            boolean calledByProcessor) {
        NioChannel sc = null;
        try {
            socketWrapper.getSocket().batchWrite();

            unreg(sk, socketWrapper, sk.readyOps());
            SendfileData sd = socketWrapper.getSendfileData();

            if (log.isTraceEnabled()) {
                log.trace("Processing send file for: " + sd.fileName);
            }

            if (sd.fchannel == null) {
                // Set up the file channel
                File f = new File(sd.fileName);
                @SuppressWarnings("resource") // Closed when channel is closed
                FileInputStream fis = new FileInputStream(f);
                sd.fchannel = fis.getChannel();
            }

            // Configure output channel
            sc = socketWrapper.getSocket();
            // TLS/SSL channel is slightly different
            WritableByteChannel wc = ((sc instanceof SecureNioChannel) ? sc : sc.getIOChannel());

            // We still have data in the buffer
            if (sc.getOutboundRemaining() > 0) {
                if (sc.flushOutbound()) {
                    socketWrapper.updateLastWrite();
                }
            } else {
                long written = sd.fchannel.transferTo(sd.pos, sd.length, wc);
                if (written > 0) {
                    sd.pos += written;
                    sd.length -= written;
                    socketWrapper.updateLastWrite();
                } else {
                    // Unusual not to be able to transfer any bytes
                    // Check the length was set correctly
                    if (sd.fchannel.size() <= sd.pos) {
                        throw new IOException(sm.getString("endpoint.sendfile.tooMuchData"));
                    }
                }
            }
            if (sd.length <= 0 && sc.getOutboundRemaining() <= 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Send file complete for: " + sd.fileName);
                }
                socketWrapper.setSendfileData(null);
                try {
                    sd.fchannel.close();
                } catch (Exception ignore) {
                    // Ignore
                }
                // For calls from outside the Poller, the caller is
                // responsible for registering the socket for the
                // appropriate event(s) if sendfile completes.
                if (!calledByProcessor) {
                    switch (sd.keepAliveState) {
                    case NONE: {
                        if (log.isTraceEnabled()) {
                            log.trace("Send file connection is being closed");
                        }
                        socketWrapper.close();
                        break;
                    }
                    case PIPELINED: {
                        if (log.isTraceEnabled()) {
                            log.trace("Connection is keep alive, processing pipe-lined data");
                        }
                        if (!socketWrapper.processSocket(SocketEvent.OPEN_READ, true)) {
                            socketWrapper.close();
                        }
                        break;
                    }
                    case OPEN: {
                        if (log.isTraceEnabled()) {
                            log.trace("Connection is keep alive, registering back for OP_READ");
                        }
                        reg(sk, socketWrapper, SelectionKey.OP_READ);
                        break;
                    }
                    }
                }
                return SendfileState.DONE;
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("OP_WRITE for sendfile: " + sd.fileName);
                }
                if (calledByProcessor) {
                    add(socketWrapper, SelectionKey.OP_WRITE);
                } else {
                    reg(sk, socketWrapper, SelectionKey.OP_WRITE);
                }
                return SendfileState.PENDING;
            }
        } catch (IOException ioe) {
            if (log.isDebugEnabled()) {
                log.debug(sm.getString("endpoint.sendfile.error"), ioe);
            }
            if (!calledByProcessor && sc != null) {
                socketWrapper.close();
            }
            return SendfileState.ERROR;
        } catch (Throwable t) {
            log.error(sm.getString("endpoint.sendfile.error"), t);
            if (!calledByProcessor && sc != null) {
                socketWrapper.close();
            }
            return SendfileState.ERROR;
        }
    }

    private void unreg(SelectionKey sk, NioSocketWrapper socketWrapper, int readyOps) {
        // This is a must, so that we don't have multiple threads messing with the socket
        reg(sk, socketWrapper, sk.interestOps() & (~readyOps));
    }

    private void reg(SelectionKey sk, NioSocketWrapper socketWrapper, int intops) {
        sk.interestOps(intops);
        socketWrapper.interestOps(intops);
    }

    private long nextExpiration = 0;

    protected void timeout(int keyCount, boolean hasEvents) {
        long now = System.currentTimeMillis();
        // This method is called on every loop of the Poller. Don't process
        // timeouts on every loop of the Poller since that would create too
        // much load and timeouts can afford to wait a few seconds.
        // However, do process timeouts if any of the following are true:
        // - the selector simply timed out (suggests there isn't much load)
        // - the nextExpiration time has passed
        // - the server socket is being closed
        if (nextExpiration > 0 && (keyCount > 0 || hasEvents) && (now < nextExpiration) && !isClosed()) {
            return;
        }
        int keycount = 0;
        try {
            for (SelectionKey key : selector.keys()) {
                keycount++;
                NioSocketWrapper socketWrapper = (NioSocketWrapper) key.attachment();
                try {
                    if (socketWrapper == null) {
                        // We don't support any keys without attachments
                        if (key.isValid()) {
                            key.cancel();
                        }
                    } else if (isClosed()) {
                        key.interestOps(0);
                        // Avoid duplicate stop calls
                        socketWrapper.interestOps(0);
                        socketWrapper.close();
                    } else if (socketWrapper.interestOpsHas(SelectionKey.OP_READ)
                            || socketWrapper.interestOpsHas(SelectionKey.OP_WRITE)) {
                        boolean readTimeout = false;
                        boolean writeTimeout = false;
                        // Check for read timeout
                        if (socketWrapper.interestOpsHas(SelectionKey.OP_READ)) {
                            long delta = now - socketWrapper.getLastRead();
                            long timeout = socketWrapper.getReadTimeout();
                            if (timeout > 0 && delta > timeout) {
                                readTimeout = true;
                            }
                        }
                        // Check for write timeout
                        if (!readTimeout && socketWrapper.interestOpsHas(SelectionKey.OP_WRITE)) {
                            long delta = now - socketWrapper.getLastWrite();
                            long timeout = socketWrapper.getWriteTimeout();
                            if (timeout > 0 && delta > timeout) {
                                writeTimeout = true;
                            }
                        }
                        if (readTimeout || writeTimeout) {
                            key.interestOps(0);
                            // Avoid duplicate timeout calls
                            socketWrapper.interestOps(0);
                            socketWrapper.setError(new SocketTimeoutException());
                            if (readTimeout && socketWrapper.readOperation != null) {
                                if (!socketWrapper.processPendingOperation()) {
                                    socketWrapper.close();
                                }
                            } else if (writeTimeout && socketWrapper.writeOperation != null) {
                                if (!socketWrapper.processPendingOperation()) {
                                    socketWrapper.close();
                                }
                            } else if (!socketWrapper.processSocket(SocketEvent.ERROR, true)) {
                                socketWrapper.close();
                            }
                        }
                    }
                } catch (CancelledKeyException ckx) {
                    if (socketWrapper != null) {
                        socketWrapper.close();
                    }
                }
            }
        } catch (ConcurrentModificationException cme) {
            // See https://bz.apache.org/bugzilla/show_bug.cgi?id=57943
            log.warn(sm.getString("endpoint.nio.timeoutCme"), cme);
        }
        // For logging purposes only
        long prevExp = nextExpiration;
        nextExpiration = System.currentTimeMillis()
                + getEndpoint().getSocketProperties().getTimeoutInterval();
        if (log.isTraceEnabled()) {
            log.trace("timeout completed: keys processed=" + keycount + "; now=" + now
                    + "; nextExpiration=" + prevExp + "; keyCount=" + keyCount + "; hasEvents="
                    + hasEvents + "; eval="
                    + ((now < prevExp) && (keyCount > 0 || hasEvents) && (!isClosed())));
        }
    }
}
