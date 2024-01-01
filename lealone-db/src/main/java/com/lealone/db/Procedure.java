/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db;

import com.lealone.sql.PreparedSQLStatement;

/**
 * Represents a procedure. Procedures are implemented for PostgreSQL compatibility.
 */
public class Procedure {

    private final String name;
    private final PreparedSQLStatement prepared;

    public Procedure(String name, PreparedSQLStatement prepared) {
        this.name = name;
        this.prepared = prepared;
    }

    public String getName() {
        return name;
    }

    public PreparedSQLStatement getPrepared() {
        return prepared;
    }
}
