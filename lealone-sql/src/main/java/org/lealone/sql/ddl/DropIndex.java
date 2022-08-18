/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.Right;
import org.lealone.db.constraint.Constraint;
import org.lealone.db.index.Index;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP INDEX
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropIndex extends SchemaStatement {

    private String indexName;
    private boolean ifExists;

    public DropIndex(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_INDEX;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    @Override
    public boolean isIfDDL() {
        return ifExists;
    }

    @Override
    public int update() {
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.INDEX, session);
        if (lock == null)
            return -1;

        Index index = schema.findIndex(session, indexName);
        if (index == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, indexName);
            }
        } else {
            Table table = index.getTable();
            session.getUser().checkRight(table, Right.ALL);
            Constraint pkConstraint = null;
            ArrayList<Constraint> constraints = table.getConstraints();
            for (int i = 0; constraints != null && i < constraints.size(); i++) {
                Constraint cons = constraints.get(i);
                if (cons.usesIndex(index)) {
                    // can drop primary key index (for compatibility)
                    if (Constraint.PRIMARY_KEY.equals(cons.getConstraintType())) {
                        pkConstraint = cons;
                    } else {
                        throw DbException.get(ErrorCode.INDEX_BELONGS_TO_CONSTRAINT_2, indexName,
                                cons.getName());
                    }
                }
            }
            table.setModified();
            if (pkConstraint != null) {
                schema.remove(session, pkConstraint, lock);
            } else {
                schema.remove(session, index, lock);
            }
        }
        return 0;
    }
}
