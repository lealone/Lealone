/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.SocketChannel;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.ExceptionUtils;
import org.apache.tomcat.util.net.NioChannel;
import org.apache.tomcat.util.net.NioEndpoint;
import org.apache.tomcat.util.net.SecureNioChannel;
import org.apache.tomcat.util.net.SocketBufferHandler;

public class TomcatNioEndpoint extends NioEndpoint {

    private static final Log log = LogFactory.getLog(TomcatNioEndpoint.class);

    private class DummyPoller extends Poller {

        NioSocketWrapper socketWrapper;

        public DummyPoller() throws IOException {
            super();
            getSelector().close(); // 不需要用了
        }

        @Override
        public void add(NioSocketWrapper socketWrapper, int interestOps) {
        }

        @Override
        public void register(final NioSocketWrapper socketWrapper) {
            this.socketWrapper = socketWrapper;
        }
    }

    private DummyPoller dummyPoller;

    public TomcatNioEndpoint() {
        try {
            dummyPoller = new DummyPoller();
            Field f = NioEndpoint.class.getDeclaredField("poller");
            f.setAccessible(true);
            f.set(this, dummyPoller);
        } catch (Exception e) {
            log.warn("new DummyPoller", e);
        }
    }

    @Override
    public void bind() throws Exception {
    }

    @Override
    protected Poller getPoller() {
        return dummyPoller;
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

    @Override
    protected NioChannel createChannel(SocketBufferHandler buffer) {
        if (isSSLEnabled()) {
            return new SecureNioChannel(buffer, this);
        }
        return new TomcatNioChannel(buffer);
    }

    public NioSocketWrapper createSocketWrapper(SocketChannel socket) {
        setSocketOptions(socket);
        return dummyPoller.socketWrapper;
    }

    @Override
    protected boolean setSocketOptions(SocketChannel socket) {
        NioSocketWrapper socketWrapper = null;
        try {
            // Allocate channel and wrapper
            NioChannel channel = null;
            if (getNioChannels() != null) {
                channel = getNioChannels().pop();
            }
            if (channel == null) {
                SocketBufferHandler bufhandler = new TomcatSocketBufferHandler(
                        socketProperties.getAppReadBufSize(), socketProperties.getAppWriteBufSize(),
                        socketProperties.getDirectBuffer());
                channel = createChannel(bufhandler);
            }
            TomcatNioSocketWrapper newWrapper = new TomcatNioSocketWrapper(channel, this);
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
            dummyPoller.register(socketWrapper);
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

    private static class TomcatSocketBufferHandler extends SocketBufferHandler {

        public TomcatSocketBufferHandler(int readBufferSize, int writeBufferSize, boolean direct) {
            // super(10 * 1024 * 1024, 10 * 1024 * 1024, true);
            super(readBufferSize, writeBufferSize, direct);
        }
    }

    private static class TomcatNioSocketWrapper extends NioSocketWrapper {

        public TomcatNioSocketWrapper(NioChannel channel, NioEndpoint endpoint) {
            super(channel, endpoint);
        }

        // @Override
        // protected void doWrite(boolean block) throws IOException {
        // // socketBufferHandler.configureWriteBufferForRead();
        // // doWrite(block, socketBufferHandler.getWriteBuffer());
        // getSocket().write(socketBufferHandler.getWriteBuffer());
        // }
        //
        // @Override
        // protected void writeBlocking(ByteBuffer from) throws IOException {
        // from.position(from.limit());
        // }
    }
}
