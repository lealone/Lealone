/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import java.util.ArrayList;

import org.lealone.command.CommandInterface;
import org.lealone.command.Prepared;
import org.lealone.dbobject.Procedure;
import org.lealone.engine.Session;
import org.lealone.expression.Parameter;
import org.lealone.util.New;

/**
 * This class represents the statement
 * PREPARE
 */
public class PrepareProcedure extends DefineCommand {

    private String procedureName;
    private Prepared prepared;

    public PrepareProcedure(Session session) {
        super(session);
    }

    public void checkParameters() {
        // no not check parameters
    }

    public int update() {
        Procedure proc = new Procedure(procedureName, prepared);
        prepared.setParameterList(parameters);
        prepared.setPrepareAlways(prepareAlways);
        prepared.prepare();
        session.addProcedure(proc);
        return 0;
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

    public void setPrepared(Prepared prep) {
        this.prepared = prep;
    }

    public ArrayList<Parameter> getParameters() {
        return New.arrayList();
    }

    public int getType() {
        return CommandInterface.PREPARE;
    }

}
