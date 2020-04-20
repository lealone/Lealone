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

    private static class ServiceExecutorInfo {
        private final String className;
        private ServiceExecutor executor;

        ServiceExecutorInfo(String className) {
            this.className = className;
        }

        // 延迟创建executor的实例，因为执行create service语句时，依赖的服务实现类还不存在
        ServiceExecutor getExecutor() {
            if (executor == null) {
                synchronized (this) {
                    try {
                        if (executor == null)
                            executor = (ServiceExecutor) Class.forName(className).newInstance();
                    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                        throw new RuntimeException("newInstance exception: " + className);
                    }
                }
            }
            return executor;
        }
    }

    private static final HashMap<String, ServiceExecutorInfo> serviceExecutors = new HashMap<>();

    public synchronized static void registerServiceExecutor(String name, String serviceExecutorClassName) {
        name = name.toUpperCase();
        serviceExecutors.put(name, new ServiceExecutorInfo(serviceExecutorClassName));
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

        ServiceExecutorInfo serviceExecutorInfo = serviceExecutors.get(serviceName);
        if (serviceExecutorInfo != null) {
            return serviceExecutorInfo.getExecutor().executeService(methodName, json);
        } else {
            throw new RuntimeException("service " + serviceName + " not found");
        }
    }
}
