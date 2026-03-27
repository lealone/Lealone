package com.lealone.http.tomcat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.apache.coyote.Adapter;
import org.apache.coyote.Response;
import org.apache.coyote.http11.AbstractHttp11Protocol;
import org.apache.coyote.http11.Http11OutputBuffer;
import org.apache.coyote.http11.Http11Processor;
import org.apache.coyote.http11.OutputFilter;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.net.AbstractEndpoint.Handler.SocketState;
import org.apache.tomcat.util.net.SocketWrapperBase;

public class TomcatHttp11Processor extends Http11Processor {

    private static final Log log = LogFactory.getLog(TomcatHttp11Processor.class);

    public TomcatHttp11Processor(AbstractHttp11Protocol<?> protocol, Adapter adapter) {
        super(protocol, adapter);
        // try {
        // Field f = Http11Processor.class.getDeclaredField("outputBuffer");
        // f.setAccessible(true);
        // Http11OutputBuffer outputBuffer = (Http11OutputBuffer) f.get(this);
        // TomcatHttp11OutputBuffer out = new TomcatHttp11OutputBuffer(response, 8192, outputBuffer);
        // f.set(this, out);
        // response.setOutputBuffer(out);
        // } catch (Exception e) {
        // log.warn("New TomcatHttp11OutputBuffer", e);
        // }
    }

    @Override
    public SocketState service(SocketWrapperBase<?> socketWrapper) throws IOException {
        // long t1 = System.nanoTime();
        SocketState state = super.service(socketWrapper);
        ((TomcatNioChannel) socketWrapper.getSocket()).batchWrite();
        // System.out.println("service: " + (System.nanoTime() - t1) / 1000);
        return state;
    }

    public static class TomcatHttp11OutputBuffer extends Http11OutputBuffer {

        protected TomcatHttp11OutputBuffer(Response response, int headerBufferSize,
                Http11OutputBuffer outputBuffer) {
            super(response, headerBufferSize);
            filterLibrary = outputBuffer.getFilters();
            activeFilters = new OutputFilter[filterLibrary.length];
        }

        public SocketWrapperBase<?> getSocketWrapper() {
            return socketWrapper;
        }

        @Override
        public void sendStatus(int status) {
            ByteBuffer g = socketWrapper.getSocketBufferHandler().getWriteBuffer();
            ByteBuffer headerBuffer = g.slice(g.position() - 32768 - 165, 165);
            try {
                Field f = Http11OutputBuffer.class.getDeclaredField("headerBuffer");
                f.setAccessible(true);
                f.set(this, headerBuffer);
            } catch (Exception e) {
                log.warn("set headerBuffer", e);
            }
            super.sendStatus(status);
        }
    }
}
