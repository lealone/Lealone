/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.sql;

import java.util.ArrayList;

import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.DbSetting;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionSetting;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.plugins.mysql.sql.ddl.CreateProcedure;
import org.lealone.plugins.mysql.sql.ddl.CreateRoutine;
import org.lealone.plugins.mysql.sql.expression.MySQLVariable;
import org.lealone.sql.SQLParserBase;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.ddl.AlterUser;
import org.lealone.sql.ddl.CreateSchema;
import org.lealone.sql.dml.SetDatabase;
import org.lealone.sql.dml.SetSession;
import org.lealone.sql.dml.SetStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.function.Function;

public class MySQLParser extends SQLParserBase {

    public MySQLParser(ServerSession session) {
        super(session);
    }

    private String identifier(String s) {
        if (database.getMode().lowerCaseIdentifiers) {
            s = s == null ? null : StringUtils.toLowerEnglish(s);
        }
        return s;
    }

    @Override
    protected StatementBase parseCreate() {
        String definer = null;
        if (readIf("DEFINER")) {
            definer = readStringOrIdentifier();
        }
        if (readIf("PROCEDURE")) {
            return parseCreateProcedure(definer);
        } else if (readIf("PROCEDURE")) {
            return parseCreateFunction(definer);
        } else {
            return super.parseCreate();
        }
    }

    @Override
    protected StatementBase parseCreateCatalog() {
        return super.parseCreateDatabase();
    }

    @Override
    protected StatementBase parseCreateDatabase() {
        return parseCreateDatabaseOrSchema();
    }

    @Override
    protected CreateSchema parseCreateSchema() {
        return parseCreateDatabaseOrSchema();
    }

    @Override
    protected StatementBase parseAlterCatalog() {
        return super.parseAlterDatabase();
    }

    @Override
    protected StatementBase parseAlterDatabase() {
        return parseAlterDatabaseOrSchema();
    }

    @Override
    protected StatementBase parseAlterSchema() {
        return parseAlterDatabaseOrSchema();
    }

    @Override
    protected StatementBase parseDropCatalog() {
        return super.parseDropDatabase();
    }

    @Override
    protected StatementBase parseDropDatabase() {
        return super.parseDropSchema();
    }

    private CreateSchema parseCreateDatabaseOrSchema() {
        CreateSchema command = super.parseCreateSchema();
        parseSchemaOption();
        return command;
    }

    private void parseSchemaOption() {
        while (true) {
            readIf("DEFAULT");
            if (readIf("CHARACTER")) {
                read("SET");
                readIfEqualOrTo();
                String charset = readStringOrIdentifier();
                setCharsetVariable(charset);
            } else if (readIf("COLLATE")) {
                readIfEqualOrTo();
                String name = readStringOrIdentifier();
                session.getDatabase().setDbSetting(session, DbSetting.COLLATION, name);
            } else if (readIf("ENCRYPTION")) {
                readIfEqualOrTo();
                readStringOrIdentifier();
            } else {
                break;
            }
        }
    }

    private StatementBase parseAlterDatabaseOrSchema() {
        String schemaName = readIdentifierWithSchema();
        session.getDatabase().getSchema(session, schemaName); // 确保存在
        parseSchemaOption();
        if (readIf("READ")) {
            read("ONLY");
            readIfEqualOrTo();
            readExpression();
        }
        return noOperation();
    }

    @Override
    protected Expression parseVariable() {
        readIf("@"); // SET @@XXX 语法
        read();
        String vname = readAliasIdentifier();
        if (vname.equalsIgnoreCase("session") || vname.equalsIgnoreCase("global")) {
            readIf(".");
            vname = readAliasIdentifier();
        }
        Expression r = new MySQLVariable(session, vname);
        if (readIf(":=")) {
            Expression value = readExpression();
            Function function = Function.getFunction(database, "SET");
            function.setParameter(0, r);
            function.setParameter(1, value);
            r = function;
        }
        return r;
    }

    @Override
    protected StatementBase parseUse() {
        String schemaName = readAliasIdentifier();
        session.setCurrentSchema(session.getDatabase().getSchema(session, schemaName));
        return noOperation();
    }

    private void readIfEqual() {
        if (!readIf("=")) {
            readIf(":=");
        }
    }

    @Override
    protected StatementBase parseSet() {
        if (readIf("@")) { // session变量
            return parseSetVariable();
        } else if (readIf("ROLE")) {
            return parseSetRole();
        } else if (readIf("DEFAULT") && readIf("ROLE")) {
            return parseSetDefaultRole();
        } else if (readIf("PASSWORD")) {
            return parseSetPassword();
        } else if (readIf("RESOURCE")) {
            return parseSetResourceGroup();
        } else if (readIf("TRANSACTION")) {
            return parseSetTransaction();
        } else if (readIf("CHARSET")) {
            return parseSetCharset();
        } else if (readIf("CHARACTER")) {
            read("SET");
            return parseSetCharset();
        } else if (readIf("NAMES")) {
            return parseSetNames();
        } else {
            if (readIf("GLOBAL") || readIf("SESSION")) {
                if (readIf("TRANSACTION")) {
                    return parseSetTransaction();
                }
            }
            if (!readIf("PERSIST")) {
                readIf("PERSIST_ONLY");
            }
            // 先看看是否是session级的参数，然后再看是否是database级的
            SetStatement command;
            try {
                command = new SetSession(session, SessionSetting.valueOf(currentToken));
            } catch (Throwable t1) {
                try {
                    command = new SetDatabase(session, DbSetting.valueOf(currentToken));
                } catch (Throwable t2) {
                    read();
                    readIfEqual();
                    readStringOrIdentifier();
                    return noOperation();
                    // throw getSyntaxError();
                }
            }
            read();
            readIfEqual();
            command.setExpression(readExpression());
            return command;
        }
    }

    private StatementBase parseSetVariable() {
        if (readIf("@")) {
            if (readIf("GLOBAL") || readIf("PERSIST") || readIf("PERSIST_ONLY") || readIf("SESSION")) {
                read(".");
            }
        }
        SetSession command = new SetSession(session, SessionSetting.VARIABLE);
        command.setString(readAliasIdentifier());
        readIfEqual();
        Expression e = readExpression();
        if (e instanceof ExpressionColumn)
            e = ValueExpression.get(ValueString.get(e.getSQL()));
        command.setExpression(e);
        return command;
    }

    private StatementBase parseSetCharset() {
        String charset;
        if (readIf("DEFAULT"))
            charset = "utf8";
        else
            charset = readString();
        setCharsetVariable(charset);
        return noOperation();
    }

    private void setCharsetVariable(String charset) {
        session.setVariable("__CHARSET__", ValueString.get(charset));
    }

    private StatementBase parseSetNames() {
        if (readIf("DEFAULT")) {
            setCharsetVariable("utf8");
        } else {
            String charset = readStringOrIdentifier();
            setCharsetVariable(charset);
            if (readIf("COLLATE")) {
                SetDatabase command = new SetDatabase(session, DbSetting.COLLATION);
                String name = readStringOrIdentifier();
                command.setString(name);
                return command;
            }
        }
        return noOperation();
    }

    private StatementBase parseSetRole() {
        return noOperation();
    }

    private StatementBase parseSetDefaultRole() {
        return noOperation();
    }

    private StatementBase parseSetPassword() {
        readIfEqual();
        AlterUser command = new AlterUser(session);
        command.setType(SQLStatement.ALTER_USER_SET_PASSWORD);
        command.setUser(session.getUser());
        command.setPassword(readExpression());
        return command;
    }

    private StatementBase parseSetResourceGroup() {
        return noOperation();
    }

    private StatementBase parseSetTransaction() {
        if (readIf("ISOLATION")) {
            read("LEVEL");
            SetSession command = new SetSession(session, SessionSetting.TRANSACTION_ISOLATION_LEVEL);
            if (readIf("SERIALIZABLE")) {
                command.setString("SERIALIZABLE");
            } else if (readIf("REPEATABLE")) {
                read("READ");
                command.setString("REPEATABLE_READ");
            } else if (readIf("READ")) {
                if (readIf("COMMITTED"))
                    command.setString("READ_COMMITTED");
                else if (readIf("UNCOMMITTED"))
                    command.setString("READ_UNCOMMITTED");
            }
            return command;
        } else if (readIf("READ")) {
            if (!readIf("WRITE"))
                read("ONLY");
        }
        return noOperation();
    }

    @Override
    protected StatementBase parseShow() {
        readIf("GLOBAL");
        readIf("SESSION");
        readIf("EXTENDED");
        readIf("FULL");
        readIf("STORAGE");

        ArrayList<Value> paramValues = Utils.newSmallArrayList();
        StringBuilder buff = new StringBuilder("SELECT ");
        if (readIf("CATALOGS")) {
            buff.append("DATABASE_NAME AS CATALOG_NAME FROM INFORMATION_SCHEMA.DATABASES");
        } else if (readIf("DATABASES") || readIf("SCHEMAS")) {
            buff.append("SCHEMA_NAME AS DATABASE_NAME FROM INFORMATION_SCHEMA.SCHEMAS");
        } else if (readIf("TABLES")) {
            parseShowTable(buff, paramValues);
        } else if (readIf("COLUMNS") || readIf("FIELDS")) {
            parseShowColumns(buff, paramValues);
        } else if (readIf("BINARY")) {
            read("LOGS");
            appendNullColumns(buff, true, "Log_name", "File_size", "Encrypted");
        } else if (readIf("BINLOG")) {
            read("EVENTS");
            appendNullColumns(buff, true, "Log_name", "Pos", "Event_type", "Server_id", "End_log_pos",
                    "Info");
        } else if (readIf("CHARSET")) {
            parseShowCharset(buff);
        } else if (readIf("CHARACTER")) {
            read("SET");
            parseShowCharset(buff);
        } else if (readIf("COLLATION")) {
            parseShowCollation(buff);
        } else if (readIf("CREATE")) {
            parseShowCreate(buff, paramValues);
        } else if (readIf("ENGINE")) {
            readUniqueIdentifier();
            buff.append("'InnoDB' AS Type, NULL AS Name, NULL AS Status FROM DUAL");
        } else if (readIf("ENGINES")) {
            buff.append("'InnoDB' AS Engine, 'DEFAULT' AS Support, "
                    + "'Supports transactions, row-level locking, and foreign keys' AS Comment, "
                    + "'YES' AS Transactions, 'YES' AS XA, 'YES' AS Savepoints FROM DUAL");
        } else if (readIf("EVENTS")) {
            appendNullColumns(buff, true, "Db", "Name", "Definer", "Time zone", "Type", "Execute at",
                    "Interval value", "Interval field", "Starts", "Ends", "Status", "Originator",
                    "character_set_client", "collation_connection", "Database Collation");
            parseShowFromOrIn(null);
            parseShowLikeOrWhere(buff, "Name", true);
        } else if (readIf("GRANTS")) {
            buff.append("* FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES");
        } else if (readIf("INDEX") || readIf("INDEXES") || readIf("KEYS")) {
            parseShowIndexes(buff, paramValues);
        } else if (readIf("MASTER")) {
            if (readIf("LOGS")) {
                appendNullColumns(buff, true, "Log_name", "File_size", "Encrypted");
            } else {
                read("STATUS");
                appendNullColumns(buff, true, "File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB",
                        "Executed_Gtid_Set");
            }
        } else if (readIf("OPEN")) {
            read("TABLES");
            parseShowOpenTables(buff, paramValues);
        } else if (readIf("PLUGINS")) {
            buff.append("'InnoDB' AS Name, 'ACTIVE' AS Status, "
                    + "'STORAGE ENGINE' AS Type, NULL AS Library, 'GPL' AS License FROM DUAL");
        } else if (readIf("PRIVILEGES")) {
            appendNullColumns(buff, true, "Privilege", "Context", "Comment");
        } else if (readIf("PROCEDURE") || readIf("FUNCTION")) {
            if (readIf("STATUS")) {
                appendNullColumns(buff, true, "Db", "Name", "Type", "Definer", "Modified", "Created",
                        "Security_type", "Comment", "character_set_client", "collation_connection",
                        "Database Collation");
                parseShowLikeOrWhere(buff, "Name", false);
            } else {
                read("CODE");
                appendNullColumns(buff, true, "Pos", "Instruction");
            }
        } else if (readIf("PROCESSLIST")) {
            appendNullColumns(buff, true, "Id", "User", "Host", "db", "Command", "Time", "State",
                    "Info");
        } else if (readIf("PROFILES") || readIf("PROFILE")) {
            appendNullColumns(buff, true, "Status ", "Duration");
        } else if (readIf("RELAYLOG")) {
            read("EVENTS");
            appendNullColumns(buff, true, "Log_name ", "Pos", "Event_type", "Server_id ", "End_log_pos",
                    "Info");
        } else if (readIf("SLAVE")) {
            if (readIf("STATUS"))
                appendNullColumns(buff, true, "Replica_IO_State", "Source_Host", "Source_User");
            else {
                read("HOSTS");
                appendNullColumns(buff, true, "Server_id ", "Host", "User", "Password ", "Port",
                        "Source_id", "Replica_UUID");
            }
        } else if (readIf("REPLICAS")) {
            appendNullColumns(buff, true, "Server_id ", "Host", "User", "Password ", "Port", "Source_id",
                    "Replica_UUID");
        } else if (readIf("REPLICA")) {
            read("STATUS");
            appendNullColumns(buff, true, "Replica_IO_State", "Source_Host", "Source_User");
        } else if (readIf("TABLE")) {
            read("STATUS");
            parseShowTableStatus(buff, paramValues);
        } else if (readIf("TRIGGERS")) {
            parseShowTriggers(buff, paramValues);
        } else if (readIf("VARIABLES") || readIf("STATUS")) {
            buff.append(
                    "NAME AS VARIABLE_NAME, VALUE AS VARIABLE_VALUE FROM INFORMATION_SCHEMA.SETTINGS");

            parseShowLikeOrWhere(buff, "VARIABLE_NAME", true);
        } else if (readIf("WARNINGS") || readIf("ERRORS")) {
            appendNullColumns(buff, true, "Level", "Code", "Message");
            if (readIf("LIMIT")) {
                readInt();
                if (readIf(","))
                    readInt();
            }
        } else if (readIf("COUNT")) {
            read("(");
            read("*");
            read(")");
            if (readIf("WARNINGS")) {
                buff.append("0 AS `@@session.warning_count` FROM DUAL");
            } else {
                read("ERRORS");
                buff.append("0 AS `@@session.error_count` FROM DUAL");
            }
        }
        boolean b = session.getAllowLiterals();
        try {
            // need to temporarily enable it, in case we are in
            // ALLOW_LITERALS_NUMBERS mode
            session.setAllowLiterals(true);
            return prepare(session, buff.toString(), paramValues);
        } finally {
            session.setAllowLiterals(b);
        }
    }

    private void parseShowCharset(StringBuilder buff) {
        buff.append("'utf8' AS Charset, 'UTF-8 Unicode' AS Description, "
                + "'utf8_general_ci' AS `Default collation`, 3 AS Maxlen FROM DUAL");
        parseShowLikeOrWhere(buff, "Charset", true);
    }

    private void parseShowCollation(StringBuilder buff) {
        buff.append("'latin1_swedish_ci' AS Collation, 'latin1' AS Charset, 8 AS Id, "
                + "'YES' AS Default, 'YES' AS Compiled, 1 AS Sortlen FROM DUAL");
        parseShowLikeOrWhere(buff, "Charset", true);
    }

    private void parseShowLikeOrWhere(StringBuilder buff, String column, boolean isWhere) {
        String w = isWhere ? " WHERE " : " AND ";
        if (readIf("LIKE")) {
            buff.append(w).append(column).append(" LIKE ");
            buff.append(readExpression().getSQL());
        } else if (readIf("WHERE")) {
            buff.append(w).append(readExpression().getSQL());
        }
    }

    private String parseShowFromOrIn(String defaultName) {
        if (readIf("FROM") || readIf("IN")) {
            return readUniqueIdentifier();
        } else {
            return defaultName;
        }
    }

    private void parseShowTable(StringBuilder buff, ArrayList<Value> paramValues) {
        String dbName = parseShowFromOrIn(getDatabaseName());
        buff.append("TABLE_NAME, TABLE_SCHEMA, TABLE_CATALOG "
                + "FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=?");

        parseShowLikeOrWhere(buff, "TABLE_NAME", false);
        buff.append(" ORDER BY TABLE_NAME");

        dbName = identifier(dbName);
        paramValues.add(ValueString.get(dbName));
    }

    private void appendNullColumns(StringBuilder buff, boolean isEmptySet, String... columns) {
        StatementBuilder b = new StatementBuilder();
        for (String c : columns) {
            b.appendExceptFirst(", ");
            b.append("null as `").append(c).append("`");
        }
        b.append(" FROM DUAL");
        if (isEmptySet)
            b.append(" WHERE 1=2");
        buff.append(b);
    }

    private void parseShowCreate(StringBuilder buff, ArrayList<Value> paramValues) {
        if (readIf("DATABASE")) {
            String schemaName = readUniqueIdentifier();
            Schema schema = session.getDatabase().getSchema(session, schemaName);
            String sql = schema.getCreateSQL();
            sql = StringUtils.quoteStringSQL(sql);
            buff.append("'" + schemaName + "' AS Database, " + sql + " AS `Create Database` FROM DUAL");
        } else if (readIf("EVENT")) {
            readUniqueIdentifier();
            if (readIf("."))
                readUniqueIdentifier();
            appendNullColumns(buff, true, "Event", "sql_mode", "time_zone", "Create Event",
                    "character_set_client", "collation_connection", "Database Collation");
        } else if (readIf("FUNCTION")) {
            readUniqueIdentifier();
            if (readIf("."))
                readUniqueIdentifier();
            appendNullColumns(buff, true, "FUNCTION", "sql_mode", "Create Function",
                    "character_set_client", "collation_connection", "Database Collation");
        } else if (readIf("PROCEDURE")) {
            readUniqueIdentifier();
            if (readIf("."))
                readUniqueIdentifier();
            appendNullColumns(buff, true, "PROCEDURE", "sql_mode", "Create Procedure",
                    "character_set_client", "collation_connection", "Database Collation");
        } else if (readIf("TABLE")) {
            Table table = readTableOrView();
            String sql = table.getCreateSQL();
            sql = StringUtils.quoteStringSQL(sql);
            buff.append("'" + table.getName() + "' AS Table, " + sql + " AS `Create Table` FROM DUAL");
        } else if (readIf("TRIGGER")) {
            readUniqueIdentifier();
            appendNullColumns(buff, true, "Trigger", "sql_mode", "SQL Original Statement",
                    "character_set_client", "collation_connection", "Database Collation", "Created");
        } else if (readIf("USER")) {
            String userName = readStringOrIdentifier();
            if (readIf("@"))
                readStringOrIdentifier();
            String columnName = "CREATE USER for " + userName;
            String sql = database.getUser(session, userName).getCreateSQL();
            sql = StringUtils.quoteStringSQL(sql);
            buff.append(sql + " AS `" + columnName + "` FROM DUAL");
        } else if (readIf("VIEW")) {
            Table table = readTableOrView();
            String sql = table.getCreateSQL();
            sql = StringUtils.quoteStringSQL(sql);
            buff.append("'" + table.getName() + "' AS View, " + sql + " AS `Create View` FROM DUAL");
        }
    }

    private void parseShowColumns(StringBuilder buff, ArrayList<Value> paramValues) {
        if (!readIf("FROM"))
            read("IN");
        String tableName = readIdentifierWithSchema();
        String dbName = parseShowFromOrIn(getDatabaseName());
        buff.append("C.COLUMN_NAME FIELD, " + "C.TYPE_NAME || '(' || C.NUMERIC_PRECISION || ')' TYPE, "
                + "C.IS_NULLABLE \"NULL\", " + "CASE (SELECT MAX(I.INDEX_TYPE_NAME) FROM "
                + "INFORMATION_SCHEMA.INDEXES I " + "WHERE I.TABLE_SCHEMA=C.TABLE_SCHEMA "
                + "AND I.TABLE_NAME=C.TABLE_NAME " + "AND I.COLUMN_NAME=C.COLUMN_NAME)"
                + "WHEN 'PRIMARY KEY' THEN 'PRI' " + "WHEN 'UNIQUE INDEX' THEN 'UNI' ELSE '' END KEY, "
                + "IFNULL(COLUMN_DEFAULT, 'NULL') DEFAULT " + "FROM INFORMATION_SCHEMA.COLUMNS C "
                + "WHERE C.TABLE_NAME=? AND C.TABLE_SCHEMA=?");

        parseShowLikeOrWhere(buff, "C.COLUMN_NAME", false);
        buff.append(" ORDER BY C.ORDINAL_POSITION");
        paramValues.add(ValueString.get(tableName));
        paramValues.add(ValueString.get(dbName));
    }

    private String getDatabaseName() {
        return session.getCurrentSchemaName();
    }

    private void parseShowIndexes(StringBuilder buff, ArrayList<Value> paramValues) {
        String tableName = parseShowFromOrIn(null);
        String dbName = parseShowFromOrIn(getDatabaseName());
        buff.append("TABLE_NAME AS Table, ");
        buff.append("NON_UNIQUE AS Non_unique, COLUMN_NAME AS Column_name ");
        buff.append("FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA=? ");
        parseShowLikeOrWhere(buff, "TABLE_NAME", false);
        if (tableName != null)
            buff.append(" AND TABLE_NAME=?");
        buff.append(" ORDER BY TABLE_NAME");

        dbName = identifier(dbName);
        paramValues.add(ValueString.get(dbName));
        if (tableName != null) {
            tableName = identifier(tableName);
            paramValues.add(ValueString.get(tableName));
        }
    }

    private void parseShowOpenTables(StringBuilder buff, ArrayList<Value> paramValues) {
        String dbName = parseShowFromOrIn(getDatabaseName());
        buff.append("TABLE_CATALOG AS Database, TABLE_NAME AS Table, ");
        buff.append("0 AS In_use, 0 AS Name_locked ");
        buff.append("FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=?");
        parseShowLikeOrWhere(buff, "TABLE_NAME", false);
        buff.append(" ORDER BY TABLE_NAME");

        dbName = identifier(dbName);
        paramValues.add(ValueString.get(dbName));
    }

    private void parseShowTableStatus(StringBuilder buff, ArrayList<Value> paramValues) {
        String dbName = parseShowFromOrIn(getDatabaseName());
        buff.append("TABLE_NAME AS Name, 'InnoDB' AS Engine, NULL AS Version, ");
        buff.append("'Fixed' AS Row_format, NULL AS Rows, 0 AS Avg_row_length, ");
        buff.append("0 AS Data_length, 0 AS Max_data_length, ");
        buff.append("0 AS Index_length, ");
        buff.append("0 AS Data_free, ");
        buff.append("NULL AS Auto_increment, ");
        buff.append("NULL AS Create_time, ");
        buff.append("NULL AS Update_time, ");
        buff.append("NULL AS Check_time, ");
        buff.append("NULL AS Collation, ");
        buff.append("NULL AS Checksum, ");
        buff.append("NULL AS Create_options, ");
        buff.append("REMARKS AS Comment ");
        buff.append("FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=?");
        parseShowLikeOrWhere(buff, "TABLE_NAME", false);
        buff.append(" ORDER BY TABLE_NAME");

        dbName = identifier(dbName);
        paramValues.add(ValueString.get(dbName));
    }

    private void parseShowTriggers(StringBuilder buff, ArrayList<Value> paramValues) {
        String dbName = parseShowFromOrIn(getDatabaseName());
        buff.append("TRIGGER_NAME AS Trigger, TRIGGER_TYPE AS Event, TABLE_NAME AS Table, ");
        buff.append("SQL AS Statement, CASEWHEN(BEFORE,'BEFORE','AFTER') AS Timing, 0 AS Created, ");
        buff.append("NULL AS sql_mode, NULL AS Definer, ");
        buff.append("'utf8mb4' AS character_set_client, ");
        buff.append("'utf8mb4_0900_ai_ci' AS collation_connection, ");
        buff.append("'utf8mb4_0900_ai_ci' AS `Database Collation` ");
        buff.append("FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_CATALOG=?");
        parseShowLikeOrWhere(buff, "TABLE_NAME", false);
        buff.append(" ORDER BY TRIGGER_NAME");

        dbName = identifier(dbName);
        paramValues.add(ValueString.get(dbName));
    }

    private StatementBase parseCreateProcedure(String definer) {
        boolean ifNotExists = readIfNotExists();
        String name = readIdentifierWithSchema();
        CreateProcedure command = new CreateProcedure(session, getSchema());
        command.setName(name);
        command.setIfNotExists(ifNotExists);
        if (readIf("(")) {
            do {
                if (readIf("IN")) {
                } else if (readIf("OUT")) {
                } else if (readIf("INOUT")) {
                }
                Column column = parseParameter();
                command.addParameter(column);
            } while (readIfMore());
        }
        parseCharacteristic(command);
        parseRoutinebody(command);
        return noOperation();
    }

    private StatementBase parseCreateFunction(String definer) {
        boolean ifNotExists = readIfNotExists();
        String name = readIdentifierWithSchema();
        CreateProcedure command = new CreateProcedure(session, getSchema());
        command.setName(name);
        command.setIfNotExists(ifNotExists);
        if (readIf("(")) {
            do {
                Column column = parseParameter();
                command.addParameter(column);
            } while (readIfMore());
        }
        read("RETURNS");
        parseColumnWithType("R");
        parseCharacteristic(command);
        parseRoutinebody(command);
        return noOperation();
    }

    private Column parseParameter() {
        String columnName = readColumnIdentifier();
        return parseColumnForTable(columnName, true);
    }

    private void parseCharacteristic(CreateRoutine command) {
        while (true) {
            if (readIf("COMMENT")) {
                readString();
            } else if (readIf("LANGUAGE")) {
                read("SQL");
            } else if (readIf("NOT")) {
                read("DETERMINISTIC");
                command.setDeterministic(false);
            } else if (readIf("DETERMINISTIC")) {
                command.setDeterministic(true);
            } else if (readIf("CONTAINS")) {
                read("SQL");
            } else if (readIf("NO")) {
                read("SQL");
            } else if (readIf("READS")) {
                read("SQL");
                read("DATA");
            } else if (readIf("MODIFIES")) {
                read("SQL");
                read("DATA");
            } else if (readIf("SQL")) {
                read("SECURITY");
                if (readIf("DEFINER")) {
                } else {
                    read("INVOKER");
                }
            } else {
                break;
            }
        }
    }

    private void parseRoutinebody(CreateRoutine command) {
        if (readIf("BEGIN")) {
            command.setPrepared(parseStatement());
            read("END");
        } else {
            readIf("RETURN");
            command.setExpression(readExpression());
        }
    }
}
