/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.apache.catalina.connector.Connector;
import org.apache.catalina.connector.OutputBuffer;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.coyote.ProtocolHandler;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import com.lealone.http.tomcat.TomcatHttp11Processor.TomcatHttp11OutputBuffer;

public class TomcatConnector extends Connector {

    private static final Log log = LogFactory.getLog(TomcatConnector.class);

    public TomcatConnector() {
        super();
    }

    public TomcatConnector(String protocol) {
        super(protocol);
    }

    public TomcatConnector(ProtocolHandler protocolHandler) {
        super(protocolHandler);
    }

    @Override
    public Request createRequest(org.apache.coyote.Request coyoteRequest) {
        return new Request(this, coyoteRequest);
    }

    // @Override
    // public Response createResponse(org.apache.coyote.Response coyoteResponse) {
    // int size = protocolHandler.getDesiredBufferSize();
    // if (size > 0) {
    // return new TomcatResponse(coyoteResponse, size);
    // } else {
    // return new TomcatResponse(coyoteResponse);
    // }
    // }

    @SuppressWarnings("unused")
    private static class TomcatResponse extends Response {

        public TomcatResponse(org.apache.coyote.Response coyoteResponse) {
            this(coyoteResponse, OutputBuffer.DEFAULT_BUFFER_SIZE);
        }

        public TomcatResponse(org.apache.coyote.Response coyoteResponse, int outputBufferSize) {
            super(coyoteResponse, outputBufferSize);
            try {
                TomcatOutputBuffer out = new TomcatOutputBuffer(8192, coyoteResponse);
                Field f = Response.class.getDeclaredField("outputBuffer");
                f.setAccessible(true);
                f.set(this, out);
            } catch (Exception e) {
                log.warn("New TomcatHttp11OutputBuffer", e);
            }
        }
    }

    private static class TomcatOutputBuffer extends OutputBuffer {

        private TomcatHttp11OutputBuffer out;

        public TomcatOutputBuffer(int size, org.apache.coyote.Response coyoteResponse) {
            super(size, coyoteResponse);
            try {
                Field f = org.apache.coyote.Response.class.getDeclaredField("outputBuffer");
                f.setAccessible(true);
                out = (TomcatHttp11OutputBuffer) f.get(coyoteResponse);
            } catch (Exception e) {
                log.warn("Get Http11OutputBuffer", e);
            }
        }

        @Override
        public void append(byte[] src, int off, int len) throws IOException {
            ByteBuffer g = out.getSocketWrapper().getSocketBufferHandler().getWriteBuffer();
            ByteBuffer bb = g.slice(g.position() + 165, len);
            g.position(g.position() + len + 165);
            bb.limit(0);
            try {
                Field f = OutputBuffer.class.getDeclaredField("bb");
                f.setAccessible(true);
                f.set(this, bb);
            } catch (Exception e) {
                log.warn("append", e);
            }
            super.append(src, off, len);
        }
    }
}
