/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

/**
 * Represents information about the properties of an index
 */
public class IndexType {

    private boolean primaryKey;
    private boolean unique;
    private boolean hash;
    private boolean scan;
    private boolean delegate;
    private boolean belongsToConstraint;

    /**
     * Create a primary key index.
     *
     * @param hash if a hash index should be used
     * @return the index type
     */
    public static IndexType createPrimaryKey(boolean hash) {
        IndexType type = new IndexType();
        type.primaryKey = true;
        type.hash = hash;
        type.unique = true;
        return type;
    }

    public static IndexType createDelegate() {
        IndexType type = new IndexType();
        type.primaryKey = true;
        type.delegate = true;
        return type;
    }

    /**
     * Create a unique index.
     *
     * @param hash if a hash index should be used
     * @return the index type
     */
    public static IndexType createUnique(boolean hash) {
        IndexType type = new IndexType();
        type.unique = true;
        type.hash = hash;
        return type;
    }

    /**
     * Create a non-unique index.
     *
     * @return the index type
     */
    public static IndexType createNonUnique() {
        return createNonUnique(false);
    }

    /**
     * Create a non-unique index.
     *
     * @param hash if a hash index should be used
     * @return the index type
     */
    public static IndexType createNonUnique(boolean hash) {
        IndexType type = new IndexType();
        type.hash = hash;
        return type;
    }

    /**
     * Create a scan pseudo-index.
     *
     * @return the index type
     */
    public static IndexType createScan() {
        IndexType type = new IndexType();
        type.scan = true;
        return type;
    }

    /**
     * Does this index belong to a primary key constraint?
     *
     * @return true if it references a primary key constraint
     */
    public boolean isPrimaryKey() {
        return primaryKey;
    }

    /**
     * Is this a unique index?
     *
     * @return true if it is
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Is this a hash index?
     *
     * @return true if it is a hash index
     */
    public boolean isHash() {
        return hash;
    }

    /**
     * Is this a table scan pseudo-index?
     *
     * @return true if it is
     */
    public boolean isScan() {
        return scan;
    }

    public boolean isDelegate() {
        return delegate;
    }

    /**
     * Sets if this index belongs to a constraint.
     *
     * @param belongsToConstraint if the index belongs to a constraint
     */
    public void setBelongsToConstraint(boolean belongsToConstraint) {
        this.belongsToConstraint = belongsToConstraint;
    }

    /**
     * If the index is created because of a constraint. Such indexes are to be
     * dropped once the constraint is dropped.
     *
     * @return if the index belongs to a constraint
     */
    public boolean getBelongsToConstraint() {
        return belongsToConstraint;
    }

    /**
     * Get the SQL snippet to create such an index.
     *
     * @return the SQL snippet
     */
    public String getSQL() {
        StringBuilder buff = new StringBuilder();
        if (primaryKey) {
            buff.append("PRIMARY KEY");
            if (hash) {
                buff.append(" HASH");
            }
        } else {
            if (unique) {
                buff.append("UNIQUE ");
            }
            if (hash) {
                buff.append("HASH ");
            }
            buff.append("INDEX");
        }
        return buff.toString();
    }
}
