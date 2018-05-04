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
package org.lealone.vertx;

import java.util.List;
import java.util.Map;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;

public class LealoneVertxClusterManager implements ClusterManager {

    @SuppressWarnings("unused")
    private Vertx vertx;

    @Override
    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getNodeID() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getNodes() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void nodeListener(NodeListener listener) {
        // TODO Auto-generated method stub

    }

    @Override
    public void join(Handler<AsyncResult<Void>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void leave(Handler<AsyncResult<Void>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isActive() {
        // TODO Auto-generated method stub
        return false;
    }

}
