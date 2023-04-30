/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.Constants;
import org.lealone.db.PluginManager;
import org.lealone.storage.StorageEngine;

public abstract class PageOperationHandlerFactory {

    protected PageOperationHandler[] pageOperationHandlers;

    protected PageOperationHandlerFactory(Map<String, String> config, PageOperationHandler[] handlers) {
        if (handlers != null) {
            setPageOperationHandlers(handlers);
            StorageEngine se = PluginManager.getPlugin(StorageEngine.class,
                    Constants.DEFAULT_STORAGE_ENGINE_NAME);
            if (se != null)
                se.setPageOperationHandlerFactory(this);
            return;
        }
        // 如果未指定处理器集，那么使用默认的
        int handlerCount;
        if (config.containsKey("page_operation_handler_count"))
            handlerCount = Math.max(1, Integer.parseInt(config.get("page_operation_handler_count")));
        else
            handlerCount = Runtime.getRuntime().availableProcessors();

        pageOperationHandlers = new PageOperationHandler[handlerCount];
        for (int i = 0; i < handlerCount; i++) {
            pageOperationHandlers[i] = new DefaultPageOperationHandler(i, handlerCount, config);
        }
        startHandlers();
    }

    public abstract PageOperationHandler getPageOperationHandler();

    public PageOperationHandler[] getPageOperationHandlers() {
        return pageOperationHandlers;
    }

    public void setPageOperationHandlers(PageOperationHandler[] handlers) {
        pageOperationHandlers = new PageOperationHandler[handlers.length];
        System.arraycopy(handlers, 0, pageOperationHandlers, 0, handlers.length);
    }

    public void startHandlers() {
        for (PageOperationHandler h : pageOperationHandlers) {
            if (h instanceof DefaultPageOperationHandler) {
                ((DefaultPageOperationHandler) h).startHandler();
            }
        }
    }

    public void stopHandlers() {
        for (PageOperationHandler h : pageOperationHandlers) {
            if (h instanceof DefaultPageOperationHandler) {
                ((DefaultPageOperationHandler) h).stopHandler();
            }
        }
    }

    public static PageOperationHandlerFactory create(Map<String, String> config) {
        return create(config, null);
    }

    public static synchronized PageOperationHandlerFactory create(Map<String, String> config,
            PageOperationHandler[] handlers) {
        if (config == null)
            config = new HashMap<>(0);
        PageOperationHandlerFactory factory = null;
        String key = "page_operation_handler_factory_type";
        String type = null; // "LoadBalance";
        if (config.containsKey(key))
            type = config.get(key);
        if (type == null || type.equalsIgnoreCase("RoundRobin"))
            factory = new RoundRobinFactory(config, handlers);
        else if (type.equalsIgnoreCase("Random"))
            factory = new RandomFactory(config, handlers);
        else if (type.equalsIgnoreCase("LoadBalance"))
            factory = new LoadBalanceFactory(config, handlers);
        else {
            throw new RuntimeException("Unknow " + key + ": " + type);
        }
        return factory;
    }

    private static class RandomFactory extends PageOperationHandlerFactory {

        private static final Random random = new Random();

        protected RandomFactory(Map<String, String> config, PageOperationHandler[] handlers) {
            super(config, handlers);
        }

        @Override
        public PageOperationHandler getPageOperationHandler() {
            int index = random.nextInt(pageOperationHandlers.length);
            return pageOperationHandlers[index];
        }
    }

    private static class RoundRobinFactory extends PageOperationHandlerFactory {

        private static final AtomicInteger index = new AtomicInteger(0);

        protected RoundRobinFactory(Map<String, String> config, PageOperationHandler[] handlers) {
            super(config, handlers);
        }

        @Override
        public PageOperationHandler getPageOperationHandler() {
            return pageOperationHandlers[index.getAndIncrement() % pageOperationHandlers.length];
        }
    }

    private static class LoadBalanceFactory extends PageOperationHandlerFactory {

        protected LoadBalanceFactory(Map<String, String> config, PageOperationHandler[] handlers) {
            super(config, handlers);
        }

        @Override
        public PageOperationHandler getPageOperationHandler() {
            long minLoad = Long.MAX_VALUE;
            int index = 0;
            for (int i = 0, size = pageOperationHandlers.length; i < size; i++) {
                long load = pageOperationHandlers[i].getLoad();
                if (load < minLoad)
                    index = i;
            }
            return pageOperationHandlers[index];
        }
    }
}
