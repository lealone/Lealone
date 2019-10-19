/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;

/**
 * Represents a database object comment.
 */
public class Comment extends DbObjectBase {

    private final DbObjectType objectType;
    private final String objectName;
    private String commentText;

    public Comment(Database database, int id, DbObject obj) {
        super(database, id, getKey(obj));
        this.objectType = obj.getType();
        this.objectName = obj.getSQL();
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.COMMENT;
    }

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("COMMENT ON ");
        buff.append(getTypeName(objectType)).append(' ').append(objectName).append(" IS ");
        if (commentText == null) {
            buff.append("NULL");
        } else {
            buff.append(StringUtils.quoteStringSQL(commentText));
        }
        return buff.toString();
    }

    @Override
    public void checkRename() {
        DbException.throwInternalError();
    }

    /**
     * Set the comment text.
     *
     * @param comment the text
     */
    public void setCommentText(String comment) {
        this.commentText = comment;
    }

    public String getCommentText() {
        return commentText;
    }

    /**
     * Get the comment key name for the given database object. This key name is
     * used internally to associate the comment to the object.
     *
     * @param obj the object
     * @return the key name
     */
    public static String getKey(DbObject obj) {
        return getTypeName(obj.getType()) + " " + obj.getSQL();
    }

    private static String getTypeName(DbObjectType type) {
        switch (type) {
        case FUNCTION_ALIAS:
            return "ALIAS";
        case TABLE_OR_VIEW:
            return "TABLE";
        case USER_DATATYPE:
            return "DOMAIN";
        default:
            return type.name();
        }
    }

}
