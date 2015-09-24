/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.ArrayList;

import org.lealone.common.util.New;
import org.lealone.db.Session;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Column;

/**
 * The data required to create a table.
 */
public class CreateTableData implements org.lealone.storage.CreateTableData {

    /**
     * The schema.
     */
    public Schema schema;

    /**
     * The table name.
     */
    public String tableName;

    /**
     * The object id.
     */
    public int id;

    /**
     * The column list.
     */
    public ArrayList<Column> columns = New.arrayList();

    /**
     * Whether this is a temporary table.
     */
    public boolean temporary;

    /**
     * Whether the table is global temporary.
     */
    public boolean globalTemporary;

    /**
     * Whether the indexes should be persisted.
     */
    public boolean persistIndexes;

    /**
     * Whether the data should be persisted.
     */
    public boolean persistData;

    /**
     * Whether to create a new table.
     */
    public boolean create;

    /**
     * The session.
     */
    public Session session;

    /**
     * The storage engine to use for creating the table.
     */
    public String storageEngine;

    /**
     * The table is hidden.
     */
    public boolean isHidden;

    public boolean isMemoryTable() {
        return !session.getDatabase().isPersistent() || isHidden || globalTemporary || temporary || !persistData
                || id <= 0;
    }
}
