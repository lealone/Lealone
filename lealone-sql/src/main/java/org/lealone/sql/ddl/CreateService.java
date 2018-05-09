/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.ArrayList;
import java.util.Map;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.common.util.New;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.ServerSession;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.Sequence;
import org.lealone.db.schema.Service;
import org.lealone.db.table.Column;
import org.lealone.db.table.CreateTableData;
import org.lealone.db.table.IndexColumn;
import org.lealone.db.table.Table;
import org.lealone.db.value.CaseInsensitiveMap;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE SERVICE
 * 
 * @author zhh
 */
public class CreateService extends SchemaStatement {

    protected final CreateTableData data = new CreateTableData();
    protected IndexColumn[] pkColumns;
    protected boolean ifNotExists;

    private final ArrayList<DefineStatement> constraintCommands = New.arrayList();
    private boolean onCommitDrop;
    private boolean onCommitTruncate;
    private String comment;
    private String packageName;
    private String implementBy;
    private final ArrayList<CreateTable> serviceMethods = New.arrayList();

    public CreateService(ServerSession session, Schema schema) {
        super(session, schema);
        data.persistIndexes = true;
        data.persistData = true;
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_TABLE;
    }

    public void setServiceName(String serviceName) {
        data.tableName = serviceName;
    }

    public void addServiceMethod(CreateTable serviceMethod) {
        serviceMethods.add(serviceMethod);
    }

    /**
     * Add a column to this table.
     *
     * @param column the column to add
     */
    public void addColumn(Column column) {
        data.columns.add(column);
    }

    /**
     * Add a constraint statement to this statement.
     * The primary key definition is one possible constraint statement.
     *
     * @param command the statement to add
     */
    public void addConstraintCommand(DefineStatement command) {
        if (command instanceof CreateIndex) {
            constraintCommands.add(command);
        } else {
            AlterTableAddConstraint con = (AlterTableAddConstraint) command;
            boolean alreadySet;
            if (con.getType() == SQLStatement.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY) {
                alreadySet = setPrimaryKeyColumns(con.getIndexColumns());
            } else {
                alreadySet = false;
            }
            if (!alreadySet) {
                constraintCommands.add(command);
            }
        }
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        // ServiceCodeGenerator.genCode(this);
        genCode();
        // TODO
        // update0();
        return 0;
    }

    public int update0() {
        Database db = session.getDatabase();
        if (!db.isPersistent()) {
            data.persistIndexes = false;
        }
        synchronized (getSchema().getLock(DbObjectType.TABLE_OR_VIEW)) {
            if (getSchema().findTableOrView(session, data.tableName) != null) {
                if (ifNotExists) {
                    return 0;
                }
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, data.tableName);
            }
            if (pkColumns != null) {
                for (Column c : data.columns) {
                    for (IndexColumn idxCol : pkColumns) {
                        if (c.getName().equals(idxCol.columnName)) {
                            c.setNullable(false);
                        }
                    }
                }
            }
            data.id = getObjectId();
            data.create = create;
            data.session = session;
            boolean isSessionTemporary = data.temporary && !data.globalTemporary;
            // if (!isSessionTemporary) {
            // db.lockMeta(session);
            // }
            Service service = new Service(getSchema(), data.id, data.tableName);
            service.setImplementBy(implementBy);
            service.setPackageName(packageName);
            Table table = getSchema().createTable(data);
            ArrayList<Sequence> sequences = New.arrayList();
            for (Column c : data.columns) {
                if (c.isAutoIncrement()) {
                    int objId = getObjectId();
                    c.convertAutoIncrementToSequence(session, getSchema(), objId, data.temporary);
                }
                Sequence seq = c.getSequence();
                if (seq != null) {
                    sequences.add(seq);
                }
            }
            table.setComment(comment);
            if (isSessionTemporary) {
                if (onCommitDrop) {
                    table.setOnCommitDrop(true);
                }
                if (onCommitTruncate) {
                    table.setOnCommitTruncate(true);
                }
                session.addLocalTempTable(table);
            } else {
                // db.lockMeta(session);
                db.addSchemaObject(session, table);
            }
            try {
                for (Column c : data.columns) {
                    c.prepareExpression(session);
                }
                for (Sequence sequence : sequences) {
                    table.addSequence(sequence);
                }
                for (DefineStatement command : constraintCommands) {
                    command.update();
                }
            } catch (DbException e) {
                db.checkPowerOff();
                db.removeSchemaObject(session, table);
                throw e;
            }
        }
        return 0;
    }

    /**
     * Sets the primary key columns, but also check if a primary key
     * with different columns is already defined.
     *
     * @param columns the primary key columns
     * @return true if the same primary key columns where already set
     */
    private boolean setPrimaryKeyColumns(IndexColumn[] columns) {
        if (pkColumns != null) {
            int len = columns.length;
            if (len != pkColumns.length) {
                throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
            }
            for (int i = 0; i < len; i++) {
                if (!columns[i].columnName.equals(pkColumns[i].columnName)) {
                    throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                }
            }
            return true;
        }
        this.pkColumns = columns;
        return false;
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        data.globalTemporary = globalTemporary;
    }

    public void setHidden(boolean isHidden) {
        data.isHidden = isHidden;
    }

    /**
     * This temporary table is dropped on commit.
     */
    public void setOnCommitDrop() {
        this.onCommitDrop = true;
    }

    /**
     * This temporary table is truncated on commit.
     */
    public void setOnCommitTruncate() {
        this.onCommitTruncate = true;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setStorageEngineName(String storageEngineName) {
        data.storageEngineName = storageEngineName;
    }

    public void setStorageEngineParams(Map<String, String> storageEngineParams) {
        if (storageEngineParams instanceof CaseInsensitiveMap) {
            data.storageEngineParams = (CaseInsensitiveMap<String>) storageEngineParams;
        } else {
            data.storageEngineParams = new CaseInsensitiveMap<>();
            data.storageEngineParams.putAll(storageEngineParams);
        }
    }

    @Override
    public boolean isReplicationStatement() {
        return true;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public void setImplementBy(String implementBy) {
        this.implementBy = implementBy;
    }

    private static String toClassName(String n) {
        String serviceName = CamelCaseHelper.toCamelFromUnderscore(n);
        String serviceNameFirstUpperCase = Character.toUpperCase(serviceName.charAt(0)) + serviceName.substring(1);
        return serviceNameFirstUpperCase;
    }

    private static String toMethodName(String n) {
        return CamelCaseHelper.toCamelFromUnderscore(n);
    }

    private static String toFieldName(String n) {
        return CamelCaseHelper.toCamelFromUnderscore(n);
    }

    void genCode() {
        StringBuilder buff = new StringBuilder();
        buff.append("package ").append(packageName).append(";\r\n");
        buff.append("\r\n");
        buff.append("import java.sql.CallableStatement;\r\n");
        buff.append("import java.sql.Connection;\r\n");
        buff.append("import java.sql.DriverManager;\r\n");
        buff.append("import java.sql.SQLException;\r\n");
        buff.append("\r\n");
        buff.append("import io.vertx.core.json.JsonArray;\r\n");
        buff.append("import io.vertx.core.json.JsonObject;\r\n");
        buff.append("\r\n");

        String serviceName = toClassName(data.tableName);

        buff.append("public interface ").append(serviceName).append(" {;\r\n");
        buff.append("\r\n");
        buff.append("    static ").append(serviceName).append(" create(String url) {\r\n");
        buff.append("        return new Proxy(url);\r\n");
        buff.append("    }\r\n");

        boolean hasNoReturnValueMethods = false;
        boolean hasWithReturnValueMethods = false;

        StringBuilder proxyMethodsBuff = new StringBuilder();

        for (CreateTable m : serviceMethods) {
            buff.append("\r\n");
            proxyMethodsBuff.append("\r\n");
            CreateTableData data = m.data;
            Column returnColumn = data.columns.get(data.columns.size() - 1);
            String returnType = returnColumn.getTable() != null ? returnColumn.getTable().getName()
                    : returnColumn.getOriginalSQL();
            returnType = toClassName(returnType);
            String methodName = toMethodName(data.tableName);
            if (returnType.equalsIgnoreCase("void")) {
                hasNoReturnValueMethods = true;
            } else {
                hasWithReturnValueMethods = true;
            }
            buff.append("    ").append(returnType).append(" ").append(methodName).append("(");

            proxyMethodsBuff.append("        @Override\r\n");
            proxyMethodsBuff.append("        public ").append(returnType).append(" ").append(methodName).append("(");

            StringBuilder argsBuff = new StringBuilder();
            argsBuff.append("            JsonArray ja = new JsonArray();\r\n");
            for (int i = 0, size = data.columns.size() - 1; i < size; i++) {
                if (i != 0) {
                    buff.append(", ");
                    proxyMethodsBuff.append(", ");
                }
                Column c = data.columns.get(i);
                String cType = c.getTable() != null ? c.getTable().getName() : c.getOriginalSQL();
                cType = toClassName(cType);
                String cName = toFieldName(c.getName());
                buff.append(cType).append(" ").append(cName);
                proxyMethodsBuff.append(cType).append(" ").append(cName);
                if (c.getTable() != null) {
                    argsBuff.append("            ja.add(JsonObject.mapFrom(").append(cName).append("));\r\n");
                } else {
                    argsBuff.append("            ja.add(").append(cName).append(");\r\n");
                }
            }
            buff.append(");\r\n");
            proxyMethodsBuff.append(") {\r\n");
            proxyMethodsBuff.append(argsBuff);
            if (returnType.equalsIgnoreCase("void")) {
                proxyMethodsBuff.append("            executeNoReturnValue(\"").append(this.data.tableName).append('.')
                        .append(data.tableName).append("\", ja.encode());\r\n");
            } else {
                proxyMethodsBuff.append("            String result = executeWithReturnValue(\"")
                        .append(this.data.tableName).append('.').append(data.tableName).append("\", ja.encode());\r\n");
                proxyMethodsBuff.append("            if (result != null) {\r\n");
                if (returnColumn.getTable() != null) {
                    proxyMethodsBuff.append("                JsonObject jo = new JsonObject(result);\r\n");
                    proxyMethodsBuff.append("                return jo.mapTo(").append(returnType)
                            .append(".class);\r\n");
                } else {
                    proxyMethodsBuff.append("                return ").append(getResultMethodName(returnType))
                            .append(";\r\n");
                }
                proxyMethodsBuff.append("            }\r\n");
                proxyMethodsBuff.append("            return null;\r\n");
            }
            proxyMethodsBuff.append("        }\r\n");
        }

        // 生成Proxy类
        buff.append("\r\n");
        buff.append("    static class Proxy implements ").append(serviceName).append(" {\r\n");
        buff.append("        private final String url;\r\n");
        if (hasNoReturnValueMethods)
            buff.append("        private static final String sqlNoReturnValue "
                    + "= \"{call executeServiceNoReturnValue(?,?)}\";\r\n");
        if (hasWithReturnValueMethods)
            buff.append("        private static final String sqlWithReturnValue "
                    + "= \"{? = call executeServiceNoReturnValue(?,?)}\";\r\n");
        buff.append("\r\n");
        buff.append("        private Proxy(String url) {\r\n");
        buff.append("            this.url = url;\r\n");
        buff.append("        }\r\n");
        buff.append("\r\n");
        buff.append(proxyMethodsBuff);

        if (hasWithReturnValueMethods) {
            buff.append("\r\n");
            buff.append("        private String executeWithReturnValue(String serviceName, String json) {\r\n");
            buff.append("            try (Connection conn = DriverManager.getConnection(url);\r\n");
            buff.append("                    CallableStatement stmt = conn.prepareCall(sqlWithReturnValue)) {\r\n");
            buff.append("                stmt.setString(2, serviceName);\r\n");
            buff.append("                stmt.setString(3, json);\r\n");
            buff.append("                stmt.registerOutParameter(1, java.sql.Types.VARCHAR);\r\n");
            buff.append("                if (stmt.execute()) {\r\n");
            buff.append("                    return stmt.getString(1);\r\n");
            buff.append("                }\r\n");
            buff.append("            } catch (SQLException e) {\r\n");
            buff.append("                e.printStackTrace();\r\n");
            buff.append("            }\r\n");
            buff.append("\r\n");
            buff.append("            return null;\r\n");
            buff.append("        }\r\n");
        }
        if (hasNoReturnValueMethods) {
            buff.append("\r\n");
            buff.append("        private void executeNoReturnValue(String serviceName, String json) {\r\n");
            buff.append("            try (Connection conn = DriverManager.getConnection(url);\r\n");
            buff.append("                    CallableStatement stmt = conn.prepareCall(sqlNoReturnValue)) {\r\n");
            buff.append("                stmt.setString(1, serviceName);\r\n");
            buff.append("                stmt.setString(2, json);\r\n");
            buff.append("                stmt.execute();\r\n");
            buff.append("            } catch (SQLException e) {\r\n");
            buff.append("                e.printStackTrace();\r\n");
            buff.append("            }\r\n");
            buff.append("        }\r\n");
        }
        buff.append("    }\r\n");
        buff.append("}\r\n");
        System.out.println(buff);
        System.out.println();
    }

    // private static class ServiceCodeGenerator {
    // static void genCode(CreateService s) {
    // }
    // }

    private static String getResultMethodName(String type) {
        type = type.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return "Boolean.valueOf(result)";
        case "BYTE":
            return "Byte.valueOf(result)";
        case "SHORT":
            return "Short.valueOf(result)";
        case "INT":
            return "Integer.valueOf(result)";
        case "LONG":
            return "Long.valueOf(result)";
        case "DECIMAL":
            return "new java.math.BigDecimal(result)";
        case "TIME":
            return "java.sql.Time.valueOf(result)";
        case "DATE":
            return "java.sql.Date.valueOf(result)";
        case "TIMESTAMP":
            return "java.sql.Timestamp.valueOf(result)";
        case "BYTES":
            // "[B", not "byte[]";
            return "result.getBytes()";
        case "UUID":
            return "java.util.UUID.fromString(result)";
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "result";
        case "BLOB":
            // "java.sql.Blob";
            throw DbException.throwInternalError("type=" + type); // return java.sql.Blob.class.getName(); // TODO
        case "CLOB":
            // "java.sql.Clob";
            throw DbException.throwInternalError("type=" + type); // return java.sql.Clob.class.getName(); // TODO
        case "DOUBLE":
            return "Double.valueOf(result)";
        case "FLOAT":
            return "Float.valueOf(result)";
        case "NULL":
            return null;
        case "JAVA_OBJECT":
            // "java.lang.Object";
            throw DbException.throwInternalError("type=" + type); // return Object.class.getName(); // TODO
        case "UNKNOWN":
            // anything
            throw DbException.throwInternalError("type=" + type);
        case "ARRAY":
            throw DbException.throwInternalError("type=" + type);
        case "RESULT_SET":
            throw DbException.throwInternalError("type=" + type); // return ResultSet.class.getName(); // TODO
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }
}
