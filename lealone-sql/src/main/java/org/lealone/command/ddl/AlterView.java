/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import org.lealone.command.CommandInterface;
import org.lealone.dbobject.Right;
import org.lealone.dbobject.table.TableView;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

/**
 * This class represents the statement
 * ALTER VIEW
 */
public class AlterView extends DefineCommand {

    private TableView view;

    public AlterView(Session session) {
        super(session);
    }

    public void setView(TableView view) {
        this.view = view;
    }

    public int update() {
        session.commit(true);
        session.getUser().checkRight(view, Right.ALL);
        DbException e = view.recompile(session, false);
        if (e != null) {
            throw e;
        }
        return 0;
    }

    public int getType() {
        return CommandInterface.ALTER_VIEW;
    }

}
