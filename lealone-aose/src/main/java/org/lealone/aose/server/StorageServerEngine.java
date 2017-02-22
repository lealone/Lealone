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
package org.lealone.aose.server;

import java.util.Map;

import org.lealone.aose.router.P2PRouter;
import org.lealone.aose.router.TransactionalRouter;
import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngineBase;
import org.lealone.sql.router.Router;
import org.lealone.sql.router.RouterHolder;

public class StorageServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "StorageServer";

    public StorageServerEngine() {
        super(NAME);
    }

    @Override
    public ProtocolServer getProtocolServer() {
        return StorageServer.instance;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        initRouter();
    }

    @Override
    public void close() {
        StorageServer.instance.stop();
    }

    @Override
    protected ProtocolServer getProtocolServer(int port) {
        return StorageServer.instance;
    }

    private static void initRouter() {
        Router r = P2PRouter.getInstance();
        RouterHolder.setRouter(new TransactionalRouter(r));
    }

}
