/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DataHandler;
import org.lealone.db.PluginBase;
import org.lealone.storage.lob.LobStorage;
import org.lealone.storage.page.PageOperationHandlerFactory;

public abstract class StorageEngineBase extends PluginBase implements StorageEngine {

    protected PageOperationHandlerFactory pohFactory;

    public StorageEngineBase(String name) {
        super(name);
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
