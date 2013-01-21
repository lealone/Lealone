/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.command.ddl;

import java.util.ArrayList;

import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.dbobject.Right;
import com.codefollower.yourbase.dbobject.RightOwner;
import com.codefollower.yourbase.dbobject.Role;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.engine.Database;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.util.New;

/**
 * This class represents the statements
 * GRANT RIGHT,
 * GRANT ROLE,
 * REVOKE RIGHT,
 * REVOKE ROLE
 */
public class GrantRevoke extends DefineCommand {

    private ArrayList<String> roleNames;
    private int operationType;
    private int rightMask;
    private final ArrayList<Table> tables = New.arrayList();
    private RightOwner grantee;

    public GrantRevoke(Session session) {
        super(session);
    }

    public void setOperationType(int operationType) {
        this.operationType = operationType;
    }

    /**
     * Add the specified right bit to the rights bitmap.
     *
     * @param right the right bit
     */
    public void addRight(int right) {
        this.rightMask |= right;
    }

    /**
     * Add the specified role to the list of roles.
     *
     * @param roleName the role
     */
    public void addRoleName(String roleName) {
        if (roleNames == null) {
            roleNames = New.arrayList();
        }
        roleNames.add(roleName);
    }

    public void setGranteeName(String granteeName) {
        Database db = session.getDatabase();
        grantee = db.findUser(granteeName);
        if (grantee == null) {
            grantee = db.findRole(granteeName);
            if (grantee == null) {
                throw DbException.get(ErrorCode.USER_OR_ROLE_NOT_FOUND_1, granteeName);
            }
        }
    }

    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        if (roleNames != null) {
            for (String name : roleNames) {
                Role grantedRole = db.findRole(name);
                if (grantedRole == null) {
                    throw DbException.get(ErrorCode.ROLE_NOT_FOUND_1, name);
                }
                if (operationType == CommandInterface.GRANT) {
                    grantRole(grantedRole);
                } else if (operationType == CommandInterface.REVOKE) {
                    revokeRole(grantedRole);
                } else {
                    DbException.throwInternalError("type=" + operationType);
                }
            }
        } else {
            if (operationType == CommandInterface.GRANT) {
                grantRight();
            } else if (operationType == CommandInterface.REVOKE) {
                revokeRight();
            } else {
                DbException.throwInternalError("type=" + operationType);
            }
        }
        return 0;
    }

    private void grantRight() {
        Database db = session.getDatabase();
        for (Table table : tables) {
            Right right = grantee.getRightForTable(table);
            if (right == null) {
                int id = getObjectId();
                right = new Right(db, id, grantee, rightMask, table);
                grantee.grantRight(table, right);
                db.addDatabaseObject(session, right);
            } else {
                right.setRightMask(right.getRightMask() | rightMask);
            }
        }
    }

    private void grantRole(Role grantedRole) {
        if (grantedRole != grantee && grantee.isRoleGranted(grantedRole)) {
            return;
        }
        if (grantee instanceof Role) {
            Role granteeRole = (Role) grantee;
            if (grantedRole.isRoleGranted(granteeRole)) {
                // cyclic role grants are not allowed
                throw DbException.get(ErrorCode.ROLE_ALREADY_GRANTED_1, grantedRole.getSQL());
            }
        }
        Database db = session.getDatabase();
        int id = getObjectId();
        Right right = new Right(db, id, grantee, grantedRole);
        db.addDatabaseObject(session, right);
        grantee.grantRole(grantedRole, right);
    }

    private void revokeRight() {
        for (Table table : tables) {
            Right right = grantee.getRightForTable(table);
            if (right == null) {
                continue;
            }
            int mask = right.getRightMask();
            int newRight = mask & ~rightMask;
            Database db = session.getDatabase();
            if (newRight == 0) {
                db.removeDatabaseObject(session, right);
            } else {
                right.setRightMask(newRight);
                db.update(session, right);
            }
        }
    }

    private void revokeRole(Role grantedRole) {
        Right right = grantee.getRightForRole(grantedRole);
        if (right == null) {
            return;
        }
        Database db = session.getDatabase();
        db.removeDatabaseObject(session, right);
    }

    public boolean isTransactional() {
        return false;
    }

    /**
     * Add the specified table to the list of tables.
     *
     * @param table the table
     */
    public void addTable(Table table) {
        tables.add(table);
    }

    public int getType() {
        return operationType;
    }

}
