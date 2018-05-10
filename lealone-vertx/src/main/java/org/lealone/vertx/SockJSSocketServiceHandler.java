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

import org.lealone.common.util.CamelCaseHelper;
import org.lealone.db.service.ServiceExecuterManager;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;

public class SockJSSocketServiceHandler implements Handler<SockJSSocket> {

    @Override
    public void handle(SockJSSocket sockJSSocket) {
        sockJSSocket.handler(buffer -> {
            String a[] = buffer.getString(0, buffer.length()).split(";");
            int type = Integer.parseInt(a[0]);
            String serviceName = CamelCaseHelper.toUnderscoreFromCamel(a[1]);
            String json = null;
            if (a.length >= 3) {
                json = a[2];
            }
            JsonArray ja = new JsonArray();
            String result = null;
            switch (type) {
            case 1:
                try {
                    result = ServiceExecuterManager.executeServiceWithReturnValue(serviceName, json);
                    ja.add(2);
                } catch (Exception e) {
                    ja.add(3);
                    result = "failed to execute service: " + serviceName + ", cause: " + e.getMessage();
                }
                break;
            default:
                ja.add(3);
                result = "unknown request type: " + type + ", serviceName: " + serviceName;
            }
            ja.add(serviceName);
            ja.add(result);
            sockJSSocket.write(Buffer.buffer(ja.toString()));
        });
    }

}
