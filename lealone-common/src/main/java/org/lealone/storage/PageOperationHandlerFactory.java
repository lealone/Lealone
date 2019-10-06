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
package org.lealone.storage;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PageOperationHandlerFactory {

    protected PageOperationHandler nodePageOperationHandler;
    protected PageOperationHandler[] leafPageOperationHandlers;;

    protected PageOperationHandlerFactory(Map<String, String> config) {
        int handlerCount;
        if (config.containsKey("page_operation_handler_count"))
            handlerCount = Integer.parseInt(config.get("page_operation_handler_count"));
        else
            handlerCount = Math.max(1, Runtime.getRuntime().availableProcessors());

        handlerCount = Math.max(1, handlerCount);

        if (handlerCount == 1) {
            nodePageOperationHandler = new DefaultPageOperationHandler("PageOperationHandler", config);
            leafPageOperationHandlers = new PageOperationHandler[] { nodePageOperationHandler };
        } else {
            nodePageOperationHandler = new DefaultPageOperationHandler("NodePageOperationHandler", config);
            handlerCount--;
            leafPageOperationHandlers = new PageOperationHandler[handlerCount];
            for (int i = 0; i < handlerCount; i++) {
                leafPageOperationHandlers[i] = new DefaultPageOperationHandler("LeafPageOperationHandler-" + i, config);
            }
        }
    }

    public abstract PageOperationHandler getPageOperationHandler();

    public void setPageOperationHandlers(PageOperationHandler[] pageOperationHandlers) {
        nodePageOperationHandler = pageOperationHandlers[0];
        int handlerCount = pageOperationHandlers.length;
        if (handlerCount == 1) {
            leafPageOperationHandlers = new PageOperationHandler[] { nodePageOperationHandler };
        } else {
            handlerCount--;
            leafPageOperationHandlers = new PageOperationHandler[handlerCount];
            for (int i = 0; i < handlerCount; i++) {
                leafPageOperationHandlers[i] = pageOperationHandlers[i];
            }
        }
    }

    public PageOperationHandler[] getLeafPageOperationHandlers() {
        return leafPageOperationHandlers;
    }

    public void setLeafPageOperationHandlers(PageOperationHandler[] leafPageOperationHandlers) {
        this.leafPageOperationHandlers = leafPageOperationHandlers;
    }

    public void setNodePageOperationHandler(PageOperationHandler handler) {
        nodePageOperationHandler = handler;
    }

    public PageOperationHandler getNodePageOperationHandler() {
        return nodePageOperationHandler != null ? nodePageOperationHandler : leafPageOperationHandlers[0];
    }

    public PageOperationHandler getPageOperationHandler(long id) {
        return leafPageOperationHandlers[((int) id) % leafPageOperationHandlers.length];
    }

    public void addPageOperation(PageOperation po) {
        Object t = Thread.currentThread();
        if (t instanceof PageOperationHandler) {
            po.run((PageOperationHandler) t);
        } else {
            PageOperationHandler handler = getPageOperationHandler();
            po.run(handler);
        }

        // PageOperationHandler handler = getPageOperationHandler();
        // if (Thread.currentThread() == handler) {
        // po.run(handler);
        // } else {
        // handler.handlePageOperation(po);
        // }
    }

    public int getPageOperationHandlerCount() {
        return leafPageOperationHandlers.length;
    }

    public void startHandlers() {
        if (nodePageOperationHandler instanceof DefaultPageOperationHandler) {
            ((DefaultPageOperationHandler) nodePageOperationHandler).start();
        }
        for (PageOperationHandler h : leafPageOperationHandlers) {
            if (h instanceof DefaultPageOperationHandler) {
                ((DefaultPageOperationHandler) h).start();
            }
        }
    }

    public void stopHandlers() {
        if (nodePageOperationHandler instanceof DefaultPageOperationHandler) {
            ((DefaultPageOperationHandler) nodePageOperationHandler).stop();
        }
        for (PageOperationHandler h : leafPageOperationHandlers) {
            if (h instanceof DefaultPageOperationHandler) {
                ((DefaultPageOperationHandler) h).stop();
            }
        }
    }

    public static PageOperationHandlerFactory create(Map<String, String> config) {
        PageOperationHandlerFactory factory = null;
        String key = "page_operation_handler_factory_type";
        String type = null;
        if (config.containsKey(key))
            type = config.get(key);
        if (type == null || type.equalsIgnoreCase("RoundRobin"))
            factory = new RoundRobinFactory(config);
        else if (type.equalsIgnoreCase("Random"))
            factory = new RandomFactory(config);
        else if (type.equalsIgnoreCase("LoadBalance"))
            factory = new LoadBalanceFactory(config);
        else {
            throw new RuntimeException("Unknow " + key + ": " + type);
        }
        return factory;
    }

    private static class RandomFactory extends PageOperationHandlerFactory {

        private static final Random random = new Random();

        protected RandomFactory(Map<String, String> config) {
            super(config);
        }

        @Override
        public PageOperationHandler getPageOperationHandler() {
            int index = random.nextInt(leafPageOperationHandlers.length);
            return leafPageOperationHandlers[index];
        }
    }

    private static class RoundRobinFactory extends PageOperationHandlerFactory {

        private static final AtomicInteger index = new AtomicInteger(0);

        protected RoundRobinFactory(Map<String, String> config) {
            super(config);
        }

        @Override
        public PageOperationHandler getPageOperationHandler() {
            return leafPageOperationHandlers[index.getAndIncrement() % leafPageOperationHandlers.length];
        }
    }

    private static class LoadBalanceFactory extends PageOperationHandlerFactory {

        protected LoadBalanceFactory(Map<String, String> config) {
            super(config);
        }

        @Override
        public PageOperationHandler getPageOperationHandler() {
            long minLoad = Long.MAX_VALUE;
            int index = 0;
            for (int i = 0, size = leafPageOperationHandlers.length; i < size; i++) {
                long load = leafPageOperationHandlers[i].getLoad();
                if (load < minLoad)
                    index = i;
            }
            return leafPageOperationHandlers[index];
        }
    }
}
