/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DataHandler;

public abstract class StorageEngineBase implements StorageEngine {

    protected final String name;
    protected Map<String, String> config;
    protected PageOperationHandlerFactory pohFactory;

    public StorageEngineBase(String name) {
        this.name = name;
        // 见PluggableEngineManager.PluggableEngineService中的注释
        StorageEngineManager.getInstance().registerEngine(this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void init(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public void close() {
    }

    @Override
    public LobStorage getLobStorage(DataHandler dataHandler, Storage storage) {
        throw DbException.getUnsupportedException("getLobStorage");
    }

    @Override
    public void setPageOperationHandlerFactory(PageOperationHandlerFactory pohFactory) {
        this.pohFactory = pohFactory;
    }

    @Override
    public PageOperationHandlerFactory getPageOperationHandlerFactory() {
        return pohFactory;
    }
}
