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
package org.lealone.db.service;

import java.util.HashMap;

public class ServiceExecuterManager {

    private ServiceExecuterManager() {
    }

    private static final HashMap<String, ServiceExecuter> serviceExecuters = new HashMap<>();
    private static final HashMap<String, String> serviceExecuterClassNames = new HashMap<>();

    public synchronized static void registerServiceExecuter(String name, ServiceExecuter serviceExecuter) {
        serviceExecuters.put(name, serviceExecuter);
    }

    public synchronized static void registerServiceExecuter(String name, String serviceExecuterClassName) {
        serviceExecuterClassNames.put(name, serviceExecuterClassName);
    }

    public synchronized void deregisterServiceExecuter(String name) {
        serviceExecuters.remove(name);
    }

    public static void executeServiceNoReturnValue(String serviceName, String json) {
        executeService(serviceName, json);
    }

    public static String executeServiceWithReturnValue(String serviceName, String json) {
        return executeService(serviceName, json);
    }

    private static String executeService(String serviceName, String json) {
        // System.out.println("serviceName=" + serviceName + ", json=" + json);
        int dotPos = serviceName.indexOf('.');
        String methodName = serviceName.substring(dotPos + 1);
        serviceName = serviceName.substring(0, dotPos);
        // serviceName = "org.lealone.test.vertx.impl." + toClassName(serviceName);

        ServiceExecuter serviceExecuter = serviceExecuters.get(serviceName);
        if (serviceExecuter == null) {
            String serviceExecuterClassName = serviceExecuterClassNames.get(serviceName);
            if (serviceExecuterClassName != null) {
                synchronized (serviceExecuterClassNames) {
                    try {
                        serviceExecuter = (ServiceExecuter) Class.forName(serviceExecuterClassName).newInstance();
                        serviceExecuters.put(serviceName, serviceExecuter);
                    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                        throw new RuntimeException("newInstance exception: " + serviceExecuterClassName);
                    }
                }
            } else {
                throw new RuntimeException("service " + serviceName + " not found");
            }
        }
        return serviceExecuter.executeService(methodName, json);
    }
}
