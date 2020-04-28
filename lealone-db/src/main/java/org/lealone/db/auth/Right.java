/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.auth;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.DbObject;
import org.lealone.db.DbObjectBase;
import org.lealone.db.DbObjectType;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;

/**
 * An access right. Rights are regular database objects, but have generated
 * names.
 */
public class Right extends DbObjectBase {

    /**
     * The right bit mask that means: selecting from a table is allowed.
     */
    public static final int SELECT = 1;

    /**
     * The right bit mask that means: deleting rows from a table is allowed.
     */
    public static final int DELETE = 2;

    /**
     * The right bit mask that means: inserting rows into a table is allowed.
     */
    public static final int INSERT = 4;

    /**
     * The right bit mask that means: updating data is allowed.
     */
    public static final int UPDATE = 8;

    /**
     * The right bit mask that means: create/alter/drop schema is allowed.
     */
    public static final int ALTER_ANY_SCHEMA = 16;

    /**
     * The right bit mask that means: select, insert, update, delete, and update
     * for this object is allowed.
     */
    public static final int ALL = SELECT | DELETE | INSERT | UPDATE;

    /**
     * To whom the right is granted.
     */
    private RightOwner grantee;

    /**
     * The granted role, or null if a right was granted.
     */
    private Role grantedRole;

    /**
     * The granted right.
     */
    private int grantedRight;

    /**
     * The object. If the right is global, this is null.
     */
    private DbObject grantedObject;

    public Right(Database db, int id, RightOwner grantee, Role grantedRole) {
        super(db, id, "RIGHT_" + id);
        this.grantee = grantee;
        this.grantedRole = grantedRole;
    }

    public Right(Database db, int id, RightOwner grantee, int grantedRight, DbObject grantedObject) {
        super(db, id, "" + id);
        this.grantee = grantee;
        this.grantedRight = grantedRight;
        this.grantedObject = grantedObject;

        // TODO 如何更优雅的处理临时对象的授权(或者内存数据库中的所有对象的授权)
        // grantedObject有可能为null，如: GRANT ALTER ANY SCHEMA
        if (grantedObject != null && (grantedObject.isTemporary() || !grantedObject.getDatabase().isPersistent()))
            setTemporary(true);
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.RIGHT;
    }

    private static boolean appendRight(StringBuilder buff, int right, int mask, String name, boolean comma) {
        if ((right & mask) != 0) {
            if (comma) {
                buff.append(", ");
            }
            buff.append(name);
            return true;
        }
        return comma;
    }

    public String getRights() {
        StringBuilder buff = new StringBuilder();
        if (grantedRight == ALL) {
            buff.append("ALL");
        } else {
            boolean comma = false;
            comma = appendRight(buff, grantedRight, SELECT, "SELECT", comma);
            comma = appendRight(buff, grantedRight, DELETE, "DELETE", comma);
            comma = appendRight(buff, grantedRight, INSERT, "INSERT", comma);
            comma = appendRight(buff, grantedRight, ALTER_ANY_SCHEMA, "ALTER ANY SCHEMA", comma);
            appendRight(buff, grantedRight, UPDATE, "UPDATE", comma);
        }
        return buff.toString();
    }

    public Role getGrantedRole() {
        return grantedRole;
    }

    public DbObject getGrantedObject() {
        return grantedObject;
    }

    public DbObject getGrantee() {
        return grantee;
    }

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder();
        buff.append("GRANT ");
        if (grantedRole != null) {
            buff.append(grantedRole.getSQL());
        } else {
            buff.append(getRights());
            if (grantedObject != null) {
                if (grantedObject instanceof Schema) {
                    buff.append(" ON SCHEMA ").append(grantedObject.getSQL());
                } else if (grantedObject instanceof Table) {
                    buff.append(" ON ").append(grantedObject.getSQL());
                }
            }
        }
        buff.append(" TO ").append(grantee.getSQL());
        return buff.toString();
    }

    @Override
    public void removeChildrenAndResources(ServerSession session) {
        if (grantedRole != null) {
            grantee.revokeRole(grantedRole);
        } else {
            grantee.revokeRight(grantedObject);
        }
        super.removeChildrenAndResources(session);
    }

    @Override
    public void invalidate() {
        grantedRole = null;
        grantedObject = null;
        grantee = null;
        super.invalidate();
    }

    @Override
    public void checkRename() {
        DbException.throwInternalError();
    }

    public void setRightMask(int rightMask) {
        grantedRight = rightMask;
    }

    public int getRightMask() {
        return grantedRight;
    }
}
