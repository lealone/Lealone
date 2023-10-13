/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.Right;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexType;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE INDEX
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateIndex extends SchemaStatement {

    private String tableName;
    private String indexName;
    private IndexColumn[] indexColumns;
    private boolean ifNotExists;
    private boolean primaryKey, unique, hash;
    private String comment;

    public CreateIndex(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_INDEX;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setIndexColumns(IndexColumn[] columns) {
        this.indexColumns = columns;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setPrimaryKey(boolean b) {
        this.primaryKey = b;
    }

    public void setUnique(boolean b) {
        this.unique = b;
    }

    public void setHash(boolean b) {
        this.hash = b;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int update() {
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.INDEX, session);
        if (lock == null)
            return -1;

        Table table = schema.getTableOrView(session, tableName);
        if (schema.findIndex(session, indexName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.INDEX_ALREADY_EXISTS_1, indexName);
        }
        if (!table.tryExclusiveLock(session))
            return -1;
        session.getUser().checkRight(table, Right.ALL);
        int id = getObjectId();
        if (indexName == null) {
            if (primaryKey) {
                indexName = table.getSchema().getUniqueIndexName(session, table,
                        Constants.PREFIX_PRIMARY_KEY);
            } else {
                indexName = table.getSchema().getUniqueIndexName(session, table, Constants.PREFIX_INDEX);
            }
        }
        IndexType indexType;
        if (primaryKey) {
            if (table.findPrimaryKey() != null) {
                throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
            }
            indexType = IndexType.createPrimaryKey(hash);
        } else if (unique) {
            indexType = IndexType.createUnique(hash);
        } else {
            indexType = IndexType.createNonUnique(hash);
        }
        IndexColumn.mapColumns(indexColumns, table);
        boolean create = !session.getDatabase().isStarting();
        table.addIndex(session, indexName, id, indexColumns, indexType, create, comment, lock);
        return 0;
    }
}
