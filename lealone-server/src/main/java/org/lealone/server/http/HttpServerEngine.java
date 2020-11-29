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
package org.lealone.server.http;

import java.util.Map;

import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngineBase;

public class HttpServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "HTTP";
    private final HttpServer httpServer = new HttpServer();

    public HttpServerEngine() {
        super(NAME);
    }

    @Override
    public ProtocolServer getProtocolServer() {
        return httpServer;
    }

    @Override
    public void init(Map<String, String> config) {
        httpServer.init(config);
    }

    @Override
    public void close() {
        httpServer.stop();
    }

    @Override
    protected ProtocolServer getProtocolServer(int port) {
        return httpServer;
    }
}
