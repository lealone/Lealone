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

public class ServiceExecutorManager {

    private ServiceExecutorManager() {
    }

    private static final HashMap<String, ServiceExecutor> serviceExecutors = new HashMap<>();
    private static final HashMap<String, String> serviceExecutorClassNames = new HashMap<>();

    public synchronized static void registerServiceExecutor(String name, ServiceExecutor serviceExecutor) {
        name = name.toUpperCase();
        serviceExecutors.put(name, serviceExecutor);
    }

    public synchronized static void registerServiceExecutor(String name, String serviceExecutorClassName) {
        name = name.toUpperCase();
        serviceExecutorClassNames.put(name, serviceExecutorClassName);
    }

    public synchronized void deregisterServiceExecutor(String name) {
        name = name.toUpperCase();
        serviceExecutors.remove(name);
    }

    public static void executeServiceNoReturnValue(String serviceName, String json) {
        executeService(serviceName, json);
    }

    public static String executeServiceWithReturnValue(String serviceName, String json) {
        return executeService(serviceName, json);
    }

    private static String executeService(String serviceName, String json) {
        serviceName = serviceName.toUpperCase();
        int dotPos = serviceName.indexOf('.');
        String methodName = serviceName.substring(dotPos + 1);
        serviceName = serviceName.substring(0, dotPos);

        ServiceExecutor serviceExecutor = serviceExecutors.get(serviceName);
        if (serviceExecutor == null) {
            String serviceExecutorClassName = serviceExecutorClassNames.get(serviceName);
            if (serviceExecutorClassName != null) {
                synchronized (serviceExecutorClassNames) {
                    serviceExecutor = serviceExecutors.get(serviceName);
                    if (serviceExecutor == null) {
                        try {
                            serviceExecutor = (ServiceExecutor) Class.forName(serviceExecutorClassName).newInstance();
                            serviceExecutors.put(serviceName, serviceExecutor);
                        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                            throw new RuntimeException("newInstance exception: " + serviceExecutorClassName);
                        }
                    }
                }
            } else {
                throw new RuntimeException("service " + serviceName + " not found");
            }
        }
        return serviceExecutor.executeService(methodName, json);
    }
}
