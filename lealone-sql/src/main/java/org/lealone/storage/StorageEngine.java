/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage;

import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Database;
import org.lealone.transaction.TransactionEngine;
import org.lealone.type.DataType;

/**
 * A class that implements this interface can create custom table
 * implementations.
 *
 * @author Sergi Vladykin
 */
public interface StorageEngine {

    /**
     * Create new table.
     *
     * @param data the data to construct the table
     * @return the created table
     */
    Table createTable(CreateTableData data);

    String getName();

    LobStorage getLobStorage();

    void close(Database db);

    void backupTo(Database db, String fileName);

    StorageMap.Builder createStorageMapBuilder(String dbName);

    TransactionEngine createTransactionEngine(DataType dataType, StorageMap.Builder mapBuilder, String hostAndPort);
}
