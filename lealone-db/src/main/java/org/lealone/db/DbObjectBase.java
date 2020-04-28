/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.util.List;

import org.lealone.db.session.ServerSession;

/**
 * The base class for all database objects.
 */
public abstract class DbObjectBase implements DbObject {

    protected Database database;
    protected int id;
    protected String name;

    protected boolean temporary;
    protected String comment;
    protected long modificationId;

    /**
     * Initialize some attributes of this object.
     *
     * @param db the database
     * @param id the object id
     * @param name the object name
     */
    protected DbObjectBase(Database database, int id, String name) {
        this.database = database;
        this.id = id;
        this.name = name;
        this.modificationId = database.getModificationMetaId();
    }

    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getSQL() {
        return database.quoteIdentifier(name);
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public List<? extends DbObject> getChildren() {
        return null;
    }

    @Override
    public void removeChildrenAndResources(ServerSession session) {
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public void rename(String newName) {
        checkRename();
        name = newName;
        setModified();
    }

    @Override
    public boolean isTemporary() {
        return temporary;
    }

    @Override
    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    @Override
    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String getComment() {
        return comment;
    }

    /**
     * Tell the object that is was modified.
     */
    public void setModified() {
        this.modificationId = database == null ? -1 : database.getNextModificationMetaId();
    }

    public long getModificationId() {
        return modificationId;
    }

    /**
     * Set the main attributes to null to make sure the object is no longer used.
     */
    @Override
    public void invalidate() {
        setModified();
        database = null;
        id = -1;
        name = null;
    }

    @Override
    public String toString() {
        return name + ":" + id + ":" + super.toString();
    }
}
