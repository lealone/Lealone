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
package com.lealone.server.http.protocol.http11;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.server.http.util.net.NioChannel;
import com.lealone.server.http.util.net.NioEndpoint;

/**
 * HTTP/1.1 protocol implementation using NIO.
 */
public class Http11NioProtocol extends AbstractHttp11Protocol<NioChannel> {

    private static final Logger log = LoggerFactory.getLogger(Http11NioProtocol.class);

    public Http11NioProtocol() {
        this(new NioEndpoint());
    }

    public Http11NioProtocol(NioEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    protected Logger getLog() {
        return log;
    }

    // -------------------- Pool setup --------------------

    public void setSelectorTimeout(long timeout) {
        ((NioEndpoint) getEndpoint()).setSelectorTimeout(timeout);
    }

    public long getSelectorTimeout() {
        return ((NioEndpoint) getEndpoint()).getSelectorTimeout();
    }

    @Override
    protected String getNamePrefix() {
        if (isSSLEnabled()) {
            return "https-" + getSslImplementationShortName() + "-nio";
        } else {
            return "http-nio";
        }
    }
}
