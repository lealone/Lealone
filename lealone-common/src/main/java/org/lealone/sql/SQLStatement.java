/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql;

/**
 * Represents a SQL statement.
 * 
 * @author H2 Group
 * @author zhh
 */
// 此接口只定义了所有子类对应的类型常量值
public interface SQLStatement extends SQLCommand {

    /**
     * The type for unknown statement.
     */
    int UNKNOWN = 0;

    // database statements

    /**
     * The type of a CREATE DATABASE statement.
     */
    int CREATE_DATABASE = 1;

    /**
     * The type of a ALTER DATABASE statement.
     */
    int ALTER_DATABASE = 2;

    /**
     * The type of a DROP DATABASE statement.
     */
    int DROP_DATABASE = 3;

    // ddl operations

    /**
     * The type of a ALTER INDEX RENAME statement.
     */
    int ALTER_INDEX_RENAME = 4;

    /**
     * The type of a ALTER SCHEMA RENAME statement.
     */
    int ALTER_SCHEMA_RENAME = 5;

    /**
     * The type of a ALTER SEQUENCE statement.
     */
    int ALTER_SEQUENCE = 6;

    /**
     * The type of a ALTER TABLE ADD CHECK statement.
     */
    int ALTER_TABLE_ADD_CONSTRAINT_CHECK = 7;

    /**
     * The type of a ALTER TABLE ADD UNIQUE statement.
     */
    int ALTER_TABLE_ADD_CONSTRAINT_UNIQUE = 8;

    /**
     * The type of a ALTER TABLE ADD FOREIGN KEY statement.
     */
    int ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL = 9;

    /**
     * The type of a ALTER TABLE ADD PRIMARY KEY statement.
     */
    int ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY = 10;

    /**
     * The type of a ALTER TABLE ADD statement.
     */
    int ALTER_TABLE_ADD_COLUMN = 11;

    /**
     * The type of a ALTER TABLE ALTER COLUMN SET NOT NULL statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_NOT_NULL = 12;

    /**
     * The type of a ALTER TABLE ALTER COLUMN SET NULL statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_NULL = 13;

    /**
     * The type of a ALTER TABLE ALTER COLUMN SET DEFAULT statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_DEFAULT = 14;

    /**
     * The type of an ALTER TABLE ALTER COLUMN statement that changes the column data type.
     */
    int ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE = 15;

    /**
     * The type of a ALTER TABLE DROP COLUMN statement.
     */
    int ALTER_TABLE_DROP_COLUMN = 16;

    /**
     * The type of a ALTER TABLE ALTER COLUMN SELECTIVITY statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_SELECTIVITY = 17;

    /**
     * The type of a ALTER TABLE DROP CONSTRAINT statement.
     */
    int ALTER_TABLE_DROP_CONSTRAINT = 18;

    /**
     * The type of a ALTER TABLE RENAME statement.
     */
    int ALTER_TABLE_RENAME = 19;

    /**
     * The type of a ALTER TABLE ALTER COLUMN RENAME statement.
     */
    int ALTER_TABLE_ALTER_COLUMN_RENAME = 20;

    /**
     * The type of a ALTER TABLE SET REFERENTIAL_INTEGRITY statement.
     */
    int ALTER_TABLE_SET_REFERENTIAL_INTEGRITY = 21;

    /**
     * The type of a ALTER USER ADMIN statement.
     */
    int ALTER_USER_ADMIN = 22;

    /**
     * The type of a ALTER USER RENAME statement.
     */
    int ALTER_USER_RENAME = 23;

    /**
     * The type of a ALTER USER SET PASSWORD statement.
     */
    int ALTER_USER_SET_PASSWORD = 24;

    /**
     * The type of a ALTER VIEW statement.
     */
    int ALTER_VIEW = 25;

    /**
     * The type of a ANALYZE statement.
     */
    int ANALYZE = 26;

    /**
     * The type of a CREATE AGGREGATE statement.
     */
    int CREATE_AGGREGATE = 30;

    /**
     * The type of a CREATE CONSTANT statement.
     */
    int CREATE_CONSTANT = 31;

    /**
     * The type of a CREATE ALIAS statement.
     */
    int CREATE_ALIAS = 32;

    /**
     * The type of a CREATE INDEX statement.
     */
    int CREATE_INDEX = 33;

    /**
     * The type of a CREATE ROLE statement.
     */
    int CREATE_ROLE = 34;

    /**
     * The type of a CREATE SCHEMA statement.
     */
    int CREATE_SCHEMA = 35;

    /**
     * The type of a CREATE SEQUENCE statement.
     */
    int CREATE_SEQUENCE = 36;

    /**
     * The type of a CREATE SERVICE statement.
     */
    int CREATE_SERVICE = 37;

    /**
     * The type of a CREATE TABLE statement.
     */
    int CREATE_TABLE = 38;

    /**
     * The type of a CREATE TRIGGER statement.
     */
    int CREATE_TRIGGER = 39;

    /**
     * The type of a CREATE USER statement.
     */
    int CREATE_USER = 40;

    /**
     * The type of a CREATE DOMAIN statement.
     */
    int CREATE_DOMAIN = 41;

    /**
     * The type of a CREATE VIEW statement.
     */
    int CREATE_VIEW = 42;

    /**
     * The type of a DEALLOCATE statement.
     */
    int DEALLOCATE = 43;

    /**
     * The type of a DROP AGGREGATE statement.
     */
    int DROP_AGGREGATE = 50;

    /**
     * The type of a DROP CONSTANT statement.
     */
    int DROP_CONSTANT = 51;

    /**
     * The type of a DROP ALIAS statement.
     */
    int DROP_ALIAS = 52;

    /**
     * The type of a DROP INDEX statement.
     */
    int DROP_INDEX = 53;

    /**
     * The type of a DROP ROLE statement.
     */
    int DROP_ROLE = 54;

    /**
     * The type of a DROP SCHEMA statement.
     */
    int DROP_SCHEMA = 55;

    /**
     * The type of a DROP SEQUENCE statement.
     */
    int DROP_SEQUENCE = 56;

    /**
     * The type of a DROP TABLE statement.
     */
    int DROP_TABLE = 57;

    /**
     * The type of a DROP TRIGGER statement.
     */
    int DROP_TRIGGER = 58;

    /**
     * The type of a DROP USER statement.
     */
    int DROP_USER = 59;

    /**
     * The type of a DROP DOMAIN statement.
     */
    int DROP_DOMAIN = 60;

    /**
     * The type of a DROP VIEW statement.
     */
    int DROP_VIEW = 61;

    /**
     * The type of a DROP SERVICE statement.
     */
    int DROP_SERVICE = 62;

    /**
     * The type of a GRANT statement.
     */
    int GRANT = 70;

    /**
     * The type of a REVOKE statement.
     */
    int REVOKE = 71;

    /**
     * The type of a PREPARE statement.
     */
    int PREPARE = 72;

    /**
     * The type of a COMMENT statement.
     */
    int COMMENT = 73;

    /**
     * The type of a TRUNCATE TABLE statement.
     */
    int TRUNCATE_TABLE = 74;

    // dml operations

    /**
     * The type of a BACKUP statement.
     */
    int BACKUP = 90;

    /**
     * The type of a CALL statement.
     */
    int CALL = 91;

    /**
     * The type of a DELETE statement.
     */
    int DELETE = 92;

    /**
     * The type of a EXECUTE statement.
     */
    int EXECUTE = 93;

    /**
     * The type of a EXPLAIN statement.
     */
    int EXPLAIN = 94;

    /**
     * The type of a INSERT statement.
     */
    int INSERT = 95;

    /**
     * The type of a MERGE statement.
     */
    int MERGE = 96;

    /**
     * The type of a no operation statement.
     */
    int NO_OPERATION = 97;

    /**
     * The type of a RUNSCRIPT statement.
     */
    int RUNSCRIPT = 98;

    /**
     * The type of a SCRIPT statement.
     */
    int SCRIPT = 99;

    /**
     * The type of a SELECT statement.
     */
    int SELECT = 100;

    /**
     * The type of a SET statement.
     */
    int SET = 101;

    /**
     * The type of a UPDATE statement.
     */
    int UPDATE = 102;

    // transaction commands

    /**
     * The type of a SET AUTOCOMMIT statement.
     */
    int SET_AUTOCOMMIT_TRUE = 120;

    /**
     * The type of a SET AUTOCOMMIT statement.
     */
    int SET_AUTOCOMMIT_FALSE = 121;

    /**
     * The type of a BEGIN {WORK|TRANSACTION} statement.
     */
    int BEGIN = 122;

    /**
     * The type of a COMMIT statement.
     */
    int COMMIT = 123;

    /**
     * The type of a ROLLBACK statement.
     */
    int ROLLBACK = 124;

    /**
     * The type of a CHECKPOINT statement.
     */
    int CHECKPOINT = 125;

    /**
     * The type of a CHECKPOINT SYNC statement.
     */
    // int CHECKPOINT_SYNC = 126; // 已经废弃，跟CHECKPOINT一样

    /**
     * The type of a SAVEPOINT statement.
     */
    int SAVEPOINT = 127;

    /**
     * The type of a ROLLBACK TO SAVEPOINT statement.
     */
    int ROLLBACK_TO_SAVEPOINT = 128;

    /**
     * The type of a PREPARE COMMIT statement.
     */
    int PREPARE_COMMIT = 129;

    /**
     * The type of a COMMIT TRANSACTION statement.
     */
    int COMMIT_TRANSACTION = 130;

    /**
     * The type of a ROLLBACK TRANSACTION statement.
     */
    int ROLLBACK_TRANSACTION = 131;

    // admin commands

    /**
     * The type of a SHUTDOWN statement.
     */
    int SHUTDOWN = 140;

    /**
     * The type of a SHUTDOWN IMMEDIATELY statement.
     */
    int SHUTDOWN_IMMEDIATELY = 141;

    /**
     * The type of a SHUTDOWN COMPACT statement.
     */
    int SHUTDOWN_COMPACT = 142;

    /**
     * The type of a SHUTDOWN DEFRAG statement.
     */
    int SHUTDOWN_DEFRAG = 143;

    /**
     * The type of a SHUTDOWN SERVER statement.
     */
    int SHUTDOWN_SERVER = 144;

    /**
     * The type of a REPAIR TABLE statement.
     */
    int REPAIR_TABLE = 145;

    String INTERNAL_SAVEPOINT = "_INTERNAL_SAVEPOINT_";
}
