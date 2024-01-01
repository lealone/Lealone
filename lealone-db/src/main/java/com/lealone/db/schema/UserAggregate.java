/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.schema;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.Utils;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.Aggregate;

/**
 * Represents a user-defined aggregate function.
 *
 * @author H2 Group
 * @author zhh
 */
public class UserAggregate extends SchemaObjectBase {

    private final String className;
    private Class<?> javaClass;

    public UserAggregate(Schema schema, int id, String name, String className, boolean force) {
        super(schema, id, name);
        this.className = className;
        if (!force) {
            getInstance();
        }
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.AGGREGATE;
    }

    public String getJavaClassName() {
        return className;
    }

    public Aggregate getInstance() {
        if (javaClass == null) {
            javaClass = Utils.loadUserClass(className);
        }
        try {
            return Utils.newInstance(javaClass);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String getCreateSQL() {
        return "CREATE FORCE AGGREGATE " + getSQL() + " FOR " + database.quoteIdentifier(className);
    }

    @Override
    public String getDropSQL() {
        return "DROP AGGREGATE IF EXISTS " + getSQL();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("AGGREGATE");
    }
}
