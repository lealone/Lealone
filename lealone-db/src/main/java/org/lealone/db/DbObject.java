/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.util.List;

import org.lealone.db.session.ServerSession;
import org.lealone.db.table.LockTable;

/**
 * A database object such as a table, an index, or a user.
 */
public interface DbObject {

    /**
     * Get the object type.
     *
     * @return the object type
     */
    DbObjectType getType();

    /**
     * Get the database.
     *
     * @return the database
     */
    Database getDatabase();

    /**
     * Get the unique object id.
     *
     * @return the object id
     */
    int getId();

    /**
     * Get the object name.
     *
     * @return the object name
     */
    String getName();

    /**
     * Get the SQL name of this object (may be quoted).
     *
     * @return the SQL name
     */
    String getSQL();

    /**
     * Construct the original CREATE ... SQL statement for this object.
     *
     * @return the SQL statement
     */
    String getCreateSQL();

    /**
     * Construct a DROP ... SQL statement for this object.
     *
     * @return the SQL statement
     */
    String getDropSQL();

    /**
     * Get the list of dependent children (for tables, this includes indexes and so on).
     *
     * @return the list of children
     */
    List<? extends DbObject> getChildren();

    /**
     * Delete all dependent children objects and resources of this object.
     *
     * @param session the session
     */
    void removeChildrenAndResources(ServerSession session, LockTable lockTable);

    /**
     * Check if renaming is allowed. Does nothing when allowed.
     */
    void checkRename();

    /**
     * Rename the object.
     *
     * @param newName the new name
     */
    void rename(String newName);

    /**
     * Check if this object is temporary (for example, a temporary table).
     *
     * @return true if is temporary
     */
    boolean isTemporary();

    /**
     * Tell this object that it is temporary or not.
     *
     * @param temporary the new value
     */
    void setTemporary(boolean temporary);

    /**
     * Change the comment of this object.
     *
     * @param comment the new comment, or null for no comment
     */
    void setComment(String comment);

    /**
     * Get the current comment of this object.
     *
     * @return the comment, or null if not set
     */
    String getComment();

    default void invalidate() {
    }
}
