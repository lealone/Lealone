/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP DATABASE
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropDatabase extends DatabaseStatement {

    private boolean ifExists;
    private boolean deleteFiles;

    public DropDatabase(ServerSession session, String dbName) {
        super(session, dbName);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_DATABASE;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public void setDeleteFiles(boolean b) {
        this.deleteFiles = b;
    }

    @Override
    public int update() {
        checkRight();
        if (LealoneDatabase.NAME.equalsIgnoreCase(dbName)) {
            throw DbException.get(ErrorCode.CANNOT_DROP_LEALONE_DATABASE);
        }
        Database db;
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        synchronized (lealoneDB.getLock(DbObjectType.DATABASE)) {
            db = lealoneDB.getDatabase(dbName);
            if (db == null) {
                if (!ifExists)
                    throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, dbName);
            } else {
                lealoneDB.removeDatabaseObject(session, db);
                if (isTargetNode(db)) {
                    // dropAllObjects(db);
                    if (deleteFiles) {
                        db.setDeleteFilesOnDisconnect(true);
                    }
                    if (db.getSessionCount() == 0) {
                        db.drop();
                    }
                }
            }
        }
        executeDatabaseStatement(db);
        return 0;
    }

    // Lealone不同于H2数据库，在H2的一个数据库中可以访问另一个数据库的对象，而Lealone不允许，
    // 所以在H2中需要一个对象一个对象地删除，这样其他数据库中的对象对他们的引用才能解除，
    // 而Lealone只要在LealoneDatabase中删除对当前数据库的引用然后删除底层的文件即可。

    // private void dropAllObjects(Database db) {
    // db.lockMeta(session);
    // db.cleanPreparedStatements();
    // // TODO local temp tables are not removed
    // for (Schema schema : db.getAllSchemas()) {
    // if (schema.canDrop()) {
    // db.removeDatabaseObject(session, schema);
    // }
    // }
    // ArrayList<Table> tables = db.getAllTablesAndViews(false);
    // for (Table t : tables) {
    // if (t.getName() != null && TableType.VIEW == t.getTableType()) {
    // db.removeSchemaObject(session, t);
    // }
    // }
    // for (Table t : tables) {
    // if (t.getName() != null && TableType.STANDARD_TABLE == t.getTableType() && !t.isHidden()) {
    // db.removeSchemaObject(session, t);
    // }
    // }
    // session.findLocalTempTable(null);
    // ArrayList<SchemaObject> list = New.arrayList();
    // list.addAll(db.getAllSchemaObjects(DbObjectType.SEQUENCE));
    // // maybe constraints and triggers on system tables will be allowed in
    // // the future
    // list.addAll(db.getAllSchemaObjects(DbObjectType.CONSTRAINT));
    // list.addAll(db.getAllSchemaObjects(DbObjectType.TRIGGER));
    // list.addAll(db.getAllSchemaObjects(DbObjectType.CONSTANT));
    // list.addAll(db.getAllSchemaObjects(DbObjectType.FUNCTION_ALIAS));
    // for (SchemaObject obj : list) {
    // if (obj.isHidden()) {
    // continue;
    // }
    // db.removeSchemaObject(session, obj);
    // }
    // for (User user : db.getAllUsers()) {
    // if (user != session.getUser()) {
    // db.removeDatabaseObject(session, user);
    // }
    // }
    // for (Role role : db.getAllRoles()) {
    // String sql = role.getCreateSQL();
    // // the role PUBLIC must not be dropped
    // if (sql != null) {
    // db.removeDatabaseObject(session, role);
    // }
    // }
    // ArrayList<DbObject> dbObjects = New.arrayList();
    // dbObjects.addAll(db.getAllRights());
    // dbObjects.addAll(db.getAllAggregates());
    // dbObjects.addAll(db.getAllUserDataTypes());
    // for (DbObject obj : dbObjects) {
    // String sql = obj.getCreateSQL();
    // // the role PUBLIC must not be dropped
    // if (sql != null) {
    // db.removeDatabaseObject(session, obj);
    // }
    // }
    // }
}
