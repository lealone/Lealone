/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.constraint;

import java.util.HashSet;
import java.util.Set;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObject;
import org.lealone.db.DbObjectType;
import org.lealone.db.index.Index;
import org.lealone.db.result.Row;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.SchemaObjectBase;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;

/**
 * The base class for constraint checking.
 */
public abstract class Constraint extends SchemaObjectBase implements Comparable<Constraint> {

    /**
     * The constraint type name for check constraints.
     */
    public static final String CHECK = "CHECK";

    /**
     * The constraint type name for referential constraints.
     */
    public static final String REFERENTIAL = "REFERENTIAL";

    /**
     * The constraint type name for unique constraints.
     */
    public static final String UNIQUE = "UNIQUE";

    /**
     * The constraint type name for primary key constraints.
     */
    public static final String PRIMARY_KEY = "PRIMARY KEY";

    /**
     * The table for which this constraint is defined.
     */
    protected Table table;

    Constraint(Schema schema, int id, String name, Table table) {
        super(schema, id, name);
        this.table = table;
        this.setTemporary(table.isTemporary());
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.CONSTRAINT;
    }

    /**
     * The constraint type name
     *
     * @return the name
     */
    public abstract String getConstraintType();

    /**
     * Check if this row fulfils the constraint.
     * This method throws an exception if not.
     *
     * @param session the session
     * @param t the table
     * @param oldRow the old row
     * @param newRow the new row
     */
    public abstract void checkRow(ServerSession session, Table t, Row oldRow, Row newRow);

    /**
     * Check if this constraint needs the specified index.
     *
     * @param index the index
     * @return true if the index is used
     */
    public abstract boolean usesIndex(Index index);

    /**
     * This index is now the owner of the specified index.
     *
     * @param index the index
     */
    public abstract void setIndexOwner(Index index);

    /**
     * Get all referenced columns.
     *
     * @param table the table
     * @return the set of referenced columns
     */
    public abstract HashSet<Column> getReferencedColumns(Table table);

    /**
     * Get the SQL statement to create this constraint.
     *
     * @return the SQL statement
     */
    public abstract String getCreateSQLWithoutIndexes();

    /**
     * Check if this constraint needs to be checked before updating the data.
     *
     * @return true if it must be checked before updating
     */
    public abstract boolean isBefore();

    /**
     * Check the existing data. This method is called if the constraint is added
     * after data has been inserted into the table.
     *
     * @param session the session
     */
    public abstract void checkExistingData(ServerSession session);

    /**
     * This method is called after a related table has changed
     * (the table was renamed, or columns have been renamed).
     */
    public abstract void rebuild();

    /**
     * Get the unique index used to enforce this constraint, or null if no index
     * is used.
     *
     * @return the index
     */
    public abstract Index getUniqueIndex();

    public Table getTable() {
        return table;
    }

    public Table getRefTable() {
        return table;
    }

    private int getConstraintTypeOrder() {
        String constraintType = getConstraintType();
        if (CHECK.equals(constraintType)) {
            return 0;
        } else if (PRIMARY_KEY.equals(constraintType)) {
            return 1;
        } else if (UNIQUE.equals(constraintType)) {
            return 2;
        } else if (REFERENTIAL.equals(constraintType)) {
            return 3;
        } else {
            throw DbException.throwInternalError("type: " + constraintType);
        }
    }

    @Override
    public int compareTo(Constraint other) {
        if (this == other) {
            return 0;
        }
        int thisType = getConstraintTypeOrder();
        int otherType = other.getConstraintTypeOrder();
        return thisType - otherType;
    }

    @Override
    public boolean isHidden() {
        return table.isHidden();
    }

    public void getDependencies(Set<DbObject> dependencies) {
    }

}
