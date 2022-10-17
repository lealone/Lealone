/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.tomcat;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.ExceptionUtils;
import org.apache.tomcat.util.compat.JreCompat;
import org.apache.tomcat.util.net.AbstractEndpoint.Handler.SocketState;
import org.apache.tomcat.util.net.NioChannel;
import org.apache.tomcat.util.net.NioEndpoint;
import org.apache.tomcat.util.net.SecureNioChannel;
import org.apache.tomcat.util.net.SocketBufferHandler;
import org.apache.tomcat.util.net.SocketEvent;
import org.apache.tomcat.util.net.SocketProcessorBase;
import org.apache.tomcat.util.net.SocketWrapperBase;

public class TomcatNioEndpoint extends NioEndpoint {

    private static final Log log = LogFactory.getLog(TomcatNioEndpoint.class);

    private class DummyPoller extends Poller {

        public DummyPoller() throws IOException {
            super();
            getSelector().close(); // 不需要用了
        }

        @Override
        public void add(NioSocketWrapper socketWrapper, int interestOps) {
        }
    }

    private Selector selector;
    private DummyPoller poller;

    public TomcatNioEndpoint() {
        try {
            poller = new DummyPoller();
        } catch (IOException e) {
            log.warn("new DummyPoller", e);
        }
    }

    public void setSelector(Selector selector) {
        this.selector = selector;
    }

    @Override
    public void bind() throws Exception {
    }

    @Override
    protected Poller getPoller() {
        return poller;
    }

    @Override
    public void startInternal() throws Exception {
    }

    private int maxKeepAliveRequests = 100; // as in Apache HTTPD server

    @Override
    public int getMaxKeepAliveRequests() {
        return maxKeepAliveRequests;
    }

    @Override
    public void setMaxKeepAliveRequests(int maxKeepAliveRequests) {
        this.maxKeepAliveRequests = maxKeepAliveRequests;
    }

    public SocketWrapperBase<NioChannel> createSocketWrapper(SocketChannel socket,
            LinkedList<NioChannel> nioChannels) {
        NioSocketWrapper socketWrapper = null;
        try {
            // Allocate channel and wrapper
            NioChannel channel = null;
            if (nioChannels != null && !nioChannels.isEmpty()) {
                channel = nioChannels.pop();
            }
            if (channel == null) {
                SocketBufferHandler bufHandler = new SocketBufferHandler(
                        socketProperties.getAppReadBufSize(), socketProperties.getAppWriteBufSize(),
                        socketProperties.getDirectBuffer());
                if (isSSLEnabled()) {
                    channel = new SecureNioChannel(bufHandler, this);
                } else {
                    channel = new NioChannel(bufHandler);
                }
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
            socketWrapper.setKeepAliveLeft(getMaxKeepAliveRequests());
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
        return socketWrapper;
    }

    @Override
    public SocketProcessorBase<NioChannel> createSocketProcessor(
            SocketWrapperBase<NioChannel> socketWrapper, SocketEvent event) {
        return new TomcatSocketProcessor(socketWrapper, event);
    }

    public class TomcatSocketProcessor extends SocketProcessor {

        public TomcatSocketProcessor(SocketWrapperBase<NioChannel> socketWrapper, SocketEvent event) {
            super(socketWrapper, event);
        }

        @Override
        protected void doRun() {
            try {
                int handshake = -1;
                try {
                    if (socketWrapper.getSocket().isHandshakeComplete()) {
                        // No TLS handshaking required. Let the handler
                        // process this socket / event combination.
                        handshake = 0;
                    } else if (event == SocketEvent.STOP || event == SocketEvent.DISCONNECT
                            || event == SocketEvent.ERROR) {
                        // Unable to complete the TLS handshake. Treat it as
                        // if the handshake failed.
                        handshake = -1;
                    } else {
                        handshake = socketWrapper.getSocket().handshake(event == SocketEvent.OPEN_READ,
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
                } catch (IOException x) {
                    handshake = -1;
                    if (log.isDebugEnabled()) {
                        log.debug("Error during SSL handshake", x);
                    }
                } catch (CancelledKeyException ckx) {
                    handshake = -1;
                }
                if (handshake == 0) {
                    SocketState state = SocketState.OPEN;
                    // Process the request from this socket
                    if (event == null) {
                        state = getHandler().process(socketWrapper, SocketEvent.OPEN_READ);
                    } else {
                        state = getHandler().process(socketWrapper, event);
                    }
                    if (state == SocketState.CLOSED) {
                        cancelledKey(getSelectionKey(), socketWrapper);
                    }
                } else if (handshake == -1) {
                    getHandler().process(socketWrapper, SocketEvent.CONNECT_FAIL);
                    cancelledKey(getSelectionKey(), socketWrapper);
                } else if (handshake == SelectionKey.OP_READ) {
                    socketWrapper.registerReadInterest();
                } else if (handshake == SelectionKey.OP_WRITE) {
                    socketWrapper.registerWriteInterest();
                }
            } catch (CancelledKeyException cx) {
                cancelledKey(getSelectionKey(), socketWrapper);
            } catch (VirtualMachineError vme) {
                ExceptionUtils.handleThrowable(vme);
            } catch (Throwable t) {
                log.error(sm.getString("endpoint.processing.fail"), t);
                cancelledKey(getSelectionKey(), socketWrapper);
            } finally {
                // socketWrapper = null;
                event = null;
                // return to cache
                if (running && processorCache != null) {
                    processorCache.push(this);
                }
            }
        }

        private SelectionKey getSelectionKey() {
            // Shortcut for Java 11 onwards
            if (JreCompat.isJre11Available()) {
                return null;
            }

            SocketChannel socketChannel = socketWrapper.getSocket().getIOChannel();
            if (socketChannel == null) {
                return null;
            }
            return socketChannel.keyFor(TomcatNioEndpoint.this.selector);
        }
    }

    private static void cancelledKey(SelectionKey sk, SocketWrapperBase<NioChannel> socketWrapper) {
        if (JreCompat.isJre11Available() && socketWrapper != null) {
            socketWrapper.close();
        } else {
            try {
                // If is important to cancel the key first, otherwise a deadlock may occur between the
                // poller select and the socket channel close which would cancel the key
                // This workaround is not needed on Java 11+
                if (sk != null) {
                    sk.attach(null);
                    if (sk.isValid()) {
                        sk.cancel();
                    }
                }
            } catch (Throwable e) {
                ExceptionUtils.handleThrowable(e);
                if (log.isDebugEnabled()) {
                    log.error(sm.getString("endpoint.debug.channelCloseFail"), e);
                }
            } finally {
                if (socketWrapper != null) {
                    socketWrapper.close();
                }
            }
        }
    }
}
