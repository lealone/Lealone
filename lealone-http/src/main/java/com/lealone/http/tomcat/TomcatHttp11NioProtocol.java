/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import org.apache.coyote.Processor;
import org.apache.coyote.http11.AbstractHttp11Protocol;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.net.AbstractEndpoint;
import org.apache.tomcat.util.net.NioChannel;
import org.apache.tomcat.util.net.NioEndpoint;

public class TomcatHttp11NioProtocol extends AbstractHttp11Protocol<NioChannel> {

    private static final Log log = LogFactory.getLog(TomcatHttp11NioProtocol.class);

    public TomcatHttp11NioProtocol() {
        super(new TomcatNioEndpoint());
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public AbstractEndpoint<NioChannel, ?> getEndpoint() {
        // Over-ridden to add cast
        return super.getEndpoint();
    }

    @Override
    protected Log getLog() {
        return log;
    }

    @Override
    protected Processor createProcessor() {
        return new TomcatHttp11Processor(this, adapter);
    }

    // -------------------- Pool setup --------------------

    public void setSelectorTimeout(long timeout) {
        ((NioEndpoint) getEndpoint()).setSelectorTimeout(timeout);
    }

    public long getSelectorTimeout() {
        return ((NioEndpoint) getEndpoint()).getSelectorTimeout();
    }

    public void setPollerThreadPriority(int threadPriority) {
        ((NioEndpoint) getEndpoint()).setPollerThreadPriority(threadPriority);
    }

    public int getPollerThreadPriority() {
        return ((NioEndpoint) getEndpoint()).getPollerThreadPriority();
    }

    // ----------------------------------------------------- JMX related methods

    @Override
    protected String getNamePrefix() {
        if (isSSLEnabled()) {
            return "https-" + getSslImplementationShortName() + "-nio";
        } else {
            return "http-nio";
        }
    }
}
