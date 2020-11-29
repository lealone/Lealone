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

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.common.util.StringUtils;
import org.lealone.db.service.Service;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;

public class HttpServiceHandler implements Handler<SockJSSocket> {

    private static final Logger logger = LoggerFactory.getLogger(HttpServiceHandler.class);

    private final String defaultDatabase;
    private final String defaultSchema;

    public HttpServiceHandler(Map<String, String> config) {
        defaultDatabase = config.get("default_database");
        defaultSchema = config.get("default_schema");
    }

    @Override
    public void handle(SockJSSocket sockJSSocket) {
        sockJSSocket.exceptionHandler(t -> {
            logger.error("sockJSSocket exception", t);
        });

        sockJSSocket.handler(buffer -> {
            String command = buffer.getString(0, buffer.length());
            Buffer ret = executeService(command);
            sockJSSocket.end(ret);
        });
    }

    public Buffer executeService(String serviceName, String methodName, Map<String, String> methodArgs) {
        String[] serviceNameArray = StringUtils.arraySplit(serviceName, '.');
        if (serviceNameArray.length == 1 && defaultDatabase != null && defaultSchema != null)
            serviceName = defaultDatabase + "." + defaultSchema + "." + serviceName;
        else if (serviceNameArray.length == 2 && defaultDatabase != null)
            serviceName = defaultDatabase + "." + serviceName;

        String result = null;
        try {
            logger.info("execute service: " + serviceName);
            result = Service.execute(serviceName, methodName, methodArgs);
        } catch (Exception e) {
            result = "failed to execute service: " + serviceName + ", cause: " + e.getMessage();
            logger.error(result, e);
        }
        return Buffer.buffer(result);
    }

    private Buffer executeService(String command) {
        String a[] = command.split(";");
        int type = Integer.parseInt(a[0]);
        String serviceName = CamelCaseHelper.toUnderscoreFromCamel(a[1]);
        String[] serviceNameArray = StringUtils.arraySplit(serviceName, '.');
        if (serviceNameArray.length == 2 && defaultDatabase != null && defaultSchema != null)
            serviceName = defaultDatabase + "." + defaultSchema + "." + serviceName;
        else if (serviceNameArray.length == 3 && defaultDatabase != null)
            serviceName = defaultDatabase + "." + serviceName;
        String json = null;
        if (a.length >= 3) {
            json = a[2];
        }
        JsonArray ja = new JsonArray();
        String result = null;
        switch (type) {
        case 1:
            try {
                logger.info("execute service: " + serviceName);
                result = Service.execute(serviceName, json);
                ja.add(2);
            } catch (Exception e) {
                ja.add(3);
                result = "failed to execute service: " + serviceName + ", cause: " + e.getMessage();
                logger.error(result, e);
            }
            break;
        default:
            ja.add(3);
            result = "unknown request type: " + type + ", serviceName: " + serviceName;
            logger.error(result);
        }
        ja.add(a[1]); // 前端传来的方法名不一定是下划线风格的，所以用最初的
        ja.add(result);
        return Buffer.buffer(ja.toString());
    }
}
