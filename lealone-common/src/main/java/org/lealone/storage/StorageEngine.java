/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import org.lealone.db.DataHandler;
import org.lealone.db.PluggableEngine;
import org.lealone.storage.lob.LobStorage;
import org.lealone.storage.page.PageOperationHandlerFactory;

public interface StorageEngine extends PluggableEngine {

    StorageBuilder getStorageBuilder();

    LobStorage getLobStorage(DataHandler dataHandler, Storage storage);

    void setPageOperationHandlerFactory(PageOperationHandlerFactory pohFactory);

    PageOperationHandlerFactory getPageOperationHandlerFactory();

}
