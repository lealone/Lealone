/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.postgresql.dbobject.table;

import java.util.HashMap;

import org.lealone.api.TableEngine;
import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.table.TableBase;
import org.lealone.dbobject.table.TableEngineManager;
import org.lealone.engine.Database;
import org.lealone.util.New;

/**
 * A table engine that internally uses the MVStore.
 */
public class PostgreSQLTableEngine implements TableEngine {
    public static final String NAME = "PostgreSQL";

    //见TableEngineManager.TableEngineService中的注释
    public PostgreSQLTableEngine() {
        TableEngineManager.registerTableEngine(this);
    }

    @Override
    public String getName() {
        return NAME;
    }

    private static HashMap<String, HashMap<TableLinkConnection, TableLinkConnection>> linkConnections;

    /**
     * Open a new connection or get an existing connection to another database.
     *
     * @param driver the database driver or null
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @return the connection
     */
    public static TableLinkConnection getLinkConnection(Database db, String driver, String url, String user, String password) {
        if (linkConnections.get(db.getName()) == null) {
            linkConnections.put(db.getName(), New.<TableLinkConnection, TableLinkConnection> hashMap());
        }
        return TableLinkConnection.open(linkConnections.get(db.getName()), driver, url, user, password,
                db.getSettings().shareLinkedConnections);
    }

    @Override
    public TableBase createTable(CreateTableData data) {
        return new PostgreSQLTable(data);
    }
}
