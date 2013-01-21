/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.command.ddl;

import java.util.ArrayList;

import com.codefollower.yourbase.dbobject.Schema;
import com.codefollower.yourbase.dbobject.table.Column;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.util.New;

/**
 * The data required to create a table.
 */
public class CreateTableData {

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
     * The table engine to use for creating the table.
     */
    public String tableEngine;

    /**
     * The table is hidden.
     */
    public boolean isHidden;

    public Table cloningTable;

}
