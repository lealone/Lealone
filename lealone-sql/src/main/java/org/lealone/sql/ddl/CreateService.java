/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.sql.ddl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.TreeSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Schema;
import org.lealone.db.service.Service;
import org.lealone.db.service.ServiceExecutor;
import org.lealone.db.service.ServiceExecutorManager;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.CreateTableData;
import org.lealone.db.value.DataType;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE SERVICE
 * 
 * @author zhh
 */
public class CreateService extends SchemaStatement {

    private final ArrayList<CreateTable> serviceMethods = new ArrayList<>();
    private String serviceName;
    private boolean ifNotExists;
    private String comment;
    private String packageName;
    private String implementBy;
    private boolean genCode;
    private String codePath;

    public CreateService(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_SERVICE;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void addServiceMethod(CreateTable serviceMethod) {
        serviceMethods.add(serviceMethod);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public void setImplementBy(String implementBy) {
        this.implementBy = implementBy;
    }

    public void setGenCode(boolean genCode) {
        this.genCode = genCode;
    }

    public void setCodePath(String codePath) {
        this.codePath = codePath;
    }

    @Override
    public boolean isReplicationStatement() {
        return true;
    }

    @Override
    public int update() {
        synchronized (getSchema().getLock(DbObjectType.SERVICE)) {
            if (getSchema().findService(serviceName) != null) {
                if (ifNotExists) {
                    return 0;
                }
                throw DbException.get(ErrorCode.SERVICE_ALREADY_EXISTS_1, serviceName);
            }
            int id = getObjectId();
            Service service = new Service(getSchema(), id, serviceName, sql);
            service.setImplementBy(implementBy);
            service.setPackageName(packageName);
            service.setComment(comment);
            Database db = session.getDatabase();
            db.addSchemaObject(session, service);
            if (genCode)
                genCode();
        }
        return 0;
    }

    public static String toClassName(String n) {
        n = CamelCaseHelper.toCamelFromUnderscore(n);
        return Character.toUpperCase(n.charAt(0)) + n.substring(1);
    }

    private static String toMethodName(String n) {
        return CamelCaseHelper.toCamelFromUnderscore(n);
    }

    private static String toFieldName(String n) {
        return CamelCaseHelper.toCamelFromUnderscore(n);
    }

    private void genCode() {
        genServiceInterfaceCode();
        genServiceExecutorCode();
    }

    private void genServiceInterfaceCode() {
        StringBuilder buff = new StringBuilder();
        StringBuilder ibuff = new StringBuilder();
        StringBuilder proxyMethodsBuff = new StringBuilder();

        TreeSet<String> importSet = new TreeSet<>();
        importSet.add("io.vertx.core.json.JsonArray");
        importSet.add("org.lealone.client.ClientServiceProxy");

        String serviceName = toClassName(this.serviceName);

        buff.append("public interface ").append(serviceName).append(" {\r\n");
        buff.append("\r\n");
        buff.append("    static ").append(serviceName).append(" create(String url) {\r\n");
        buff.append("        return new Proxy(url);\r\n");
        buff.append("    }\r\n");

        for (CreateTable m : serviceMethods) {
            buff.append("\r\n");
            proxyMethodsBuff.append("\r\n");
            CreateTableData data = m.data;
            Column returnColumn = data.columns.get(data.columns.size() - 1);
            String returnType = getTypeName(returnColumn, importSet);
            String methodName = toMethodName(data.tableName);
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
                String cType = getTypeName(c, importSet);
                String cName = toFieldName(c.getName());
                buff.append(cType).append(" ").append(cName);
                proxyMethodsBuff.append(cType).append(" ").append(cName);
                if (c.getTable() != null) {
                    importSet.add("io.vertx.core.json.JsonObject");
                    argsBuff.append("            ja.add(JsonObject.mapFrom(").append(cName).append("));\r\n");
                } else {
                    argsBuff.append("            ja.add(").append(cName).append(");\r\n");
                }
            }
            buff.append(");\r\n");
            proxyMethodsBuff.append(") {\r\n");
            proxyMethodsBuff.append(argsBuff);
            if (returnType.equalsIgnoreCase("void")) {
                proxyMethodsBuff.append("            ClientServiceProxy.executeNoReturnValue(url, \"")
                        .append(this.serviceName).append('.').append(data.tableName).append("\", ja.encode());\r\n");
            } else {
                proxyMethodsBuff.append("            String result = ClientServiceProxy.executeWithReturnValue(url, \"")
                        .append(this.serviceName).append('.').append(data.tableName).append("\", ja.encode());\r\n");
                proxyMethodsBuff.append("            if (result != null) {\r\n");
                if (returnColumn.getTable() != null) {
                    importSet.add("io.vertx.core.json.JsonObject");
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
        buff.append("\r\n");
        buff.append("        private final String url;\r\n");
        buff.append("\r\n");
        buff.append("        private Proxy(String url) {\r\n");
        buff.append("            this.url = url;\r\n");
        buff.append("        }\r\n");
        buff.append(proxyMethodsBuff);
        buff.append("    }\r\n");
        buff.append("}\r\n");

        ibuff.append("package ").append(packageName).append(";\r\n");
        ibuff.append("\r\n");
        for (String i : importSet) {
            ibuff.append("import ").append(i).append(";\r\n");
        }
        ibuff.append("\r\n");

        ibuff.append("/**\r\n");
        ibuff.append(" * Service interface for '").append(this.serviceName.toLowerCase()).append("'.\r\n");
        ibuff.append(" *\r\n");
        ibuff.append(" * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.\r\n");
        ibuff.append(" */\r\n");
        // System.out.println(ibuff);
        // System.out.println(buff);
        // System.out.println();

        writeFile(codePath, packageName, serviceName, ibuff, buff);
    }

    private void genServiceExecutorCode() {
        StringBuilder buff = new StringBuilder();
        StringBuilder ibuff = new StringBuilder();

        TreeSet<String> importSet = new TreeSet<>();
        importSet.add(ServiceExecutor.class.getName());
        String serviceImplementClassName = implementBy;
        if (implementBy != null) {
            if (implementBy.startsWith(packageName)) {
                serviceImplementClassName = implementBy.substring(packageName.length() + 1);
            } else {
                int lastDotPos = implementBy.lastIndexOf('.');
                if (lastDotPos > 0) {
                    serviceImplementClassName = implementBy.substring(lastDotPos + 1);
                    importSet.add(implementBy);
                }
            }
        }
        String serviceName = toClassName(this.serviceName);
        String className = serviceName + "Executor";

        buff.append("public class ").append(className).append(" implements ServiceExecutor {\r\n");
        buff.append("\r\n");
        buff.append("    private final ").append(serviceImplementClassName).append(" s = new ")
                .append(serviceImplementClassName).append("();\r\n");
        buff.append("\r\n");
        buff.append("    public ").append(className).append("() {\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");
        buff.append("    @Override\r\n");
        buff.append("    public String executeService(String methodName, String json) {\r\n");
        // 提前看一下是否用到JsonArray
        for (CreateTable m : serviceMethods) {
            if (m.data.columns.size() - 1 > 0) {
                buff.append("        JsonArray ja = null;\r\n");
                break;
            }
        }
        buff.append("        switch (methodName) {\r\n");

        boolean hasNoReturnValueMethods = false;
        int index = 0;
        for (CreateTable m : serviceMethods) {
            index++;
            // switch语句不同case代码块的本地变量名不能相同
            String resultVarName = "result" + index;
            CreateTableData data = m.data;

            Column returnColumn = data.columns.get(data.columns.size() - 1);
            String returnType = getTypeName(returnColumn, importSet);
            if (returnType.equalsIgnoreCase("void")) {
                hasNoReturnValueMethods = true;
            }
            StringBuilder argsBuff = new StringBuilder();
            String methodName = toMethodName(data.tableName);
            buff.append("        case \"").append(data.tableName).append("\":\r\n");
            // 有参数，参数放在一个json数组中
            int size = data.columns.size() - 1;
            if (size > 0) {
                importSet.add("io.vertx.core.json.JsonArray");
                buff.append("            ja = new JsonArray(json);\r\n");
                for (int i = 0; i < size; i++) {
                    if (i != 0) {
                        buff.append(", ");
                        argsBuff.append(", ");
                    }
                    Column c = data.columns.get(i);
                    String cType = getTypeName(c, importSet);
                    String cName = "p_" + toFieldName(c.getName()) + index;
                    buff.append("            ").append(cType).append(" ").append(cName).append(" = ")
                            .append(getJsonArrayMethodName(cType, i)).append(";\r\n");
                    argsBuff.append(cName);
                }
            }
            boolean isVoid = returnType.equalsIgnoreCase("void");
            buff.append("            ");
            if (!isVoid) {
                buff.append(returnType).append(" ").append(resultVarName).append(" = ");
            }
            buff.append("this.s.").append(methodName).append("(").append(argsBuff).append(");\r\n");
            if (!isVoid) {
                buff.append("            if (").append(resultVarName).append(" == null)\r\n");
                buff.append("                return null;\r\n");
                if (returnColumn.getTable() != null) {
                    importSet.add("io.vertx.core.json.JsonObject");
                    buff.append("            return JsonObject.mapFrom(").append(resultVarName)
                            .append(").encode();\r\n");
                } else if (!returnType.equalsIgnoreCase("string")) {
                    buff.append("            return ").append(resultVarName).append(".toString();\r\n");
                } else {
                    buff.append("            return ").append(resultVarName).append(";\r\n");
                }
            } else {
                buff.append("            break;\r\n");
            }
        }
        buff.append("        default:\r\n");
        buff.append("            throw new RuntimeException(\"no method: \" + methodName);\r\n");
        buff.append("        }\r\n");
        if (hasNoReturnValueMethods)
            buff.append("        return NO_RETURN_VALUE;\r\n");

        buff.append("    }\r\n");
        buff.append("}\r\n");

        ibuff.append("package ").append(getExecutorPackageName()).append(";\r\n");
        ibuff.append("\r\n");
        for (String i : importSet) {
            ibuff.append("import ").append(i).append(";\r\n");
        }
        ibuff.append("\r\n");

        ibuff.append("/**\r\n");
        ibuff.append(" * Service executor for '").append(this.serviceName.toLowerCase()).append("'.\r\n");
        ibuff.append(" *\r\n");
        ibuff.append(" * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.\r\n");
        ibuff.append(" */\r\n");

        writeFile(codePath, getExecutorPackageName(), className, ibuff, buff);
        registerServiceExecutor(className);
    }

    private void registerServiceExecutor(String executorName) {
        String fullName = getExecutorPackageName() + "." + executorName;
        ServiceExecutorManager.registerServiceExecutor(serviceName, fullName);
    }

    private String getExecutorPackageName() {
        return packageName + ".executor";
    }

    public static void writeFile(String codePath, String packageName, String className, StringBuilder... buffArray) {
        String path = codePath;
        if (!path.endsWith(File.separator))
            path = path + File.separator;
        path = path.replace('/', File.separatorChar);
        path = path + packageName.replace('.', File.separatorChar) + File.separatorChar;
        try {
            if (!new File(path).exists()) {
                new File(path).mkdirs();
            }
            Charset utf8 = Charset.forName("UTF-8");
            BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(path + className + ".java"));
            for (StringBuilder buff : buffArray) {
                file.write(buff.toString().getBytes(utf8));
            }
            file.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Failed to genJavaCode, path = " + path);
        }
    }

    private static String getTypeName(Column c, TreeSet<String> importSet) {
        String cType;
        if (c.getTable() != null) {
            cType = c.getTable().getName();
            cType = toClassName(cType);
            String packageName = c.getTable().getPackageName();
            if (packageName != null)
                cType = packageName + "." + cType;
        } else {
            // cType = c.getOriginalSQL();
            cType = DataType.getTypeClassName(c.getType());
        }
        int lastDotPos = cType.lastIndexOf('.');
        if (lastDotPos > 0) {
            if (cType.startsWith("java.lang.")) {
                cType = cType.substring(10);
            } else {
                importSet.add(cType);
                cType = cType.substring(lastDotPos + 1);
            }
        }
        // 把java.lang.Void转成void，这样就不用加return语句
        if (cType.equalsIgnoreCase("void")) {
            cType = "void";
        }
        return cType;
    }

    private static String getResultMethodName(String type) {
        type = type.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return "Boolean.valueOf(result)";
        case "BYTE":
            return "Byte.valueOf(result)";
        case "SHORT":
            return "Short.valueOf(result)";
        case "INTEGER":
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

    private static String m(String str, int i) {
        return str + "(ja.getValue(" + i + ").toString())";
    }

    // TODO 判断具体类型调用合适的JsonArray方法
    private static String getJsonArrayMethodName(String type0, int i) {
        String type = type0.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return m("Boolean.valueOf", i);
        case "BYTE":
            return m("Byte.valueOf", i);
        case "SHORT":
            return m("Short.valueOf", i);
        case "INT":
            return m("Integer.valueOf", i);
        case "LONG":
            return m("Long.valueOf", i);
        case "DECIMAL":
            return m("new java.math.BigDecimal", i);
        case "TIME":
            return m("java.sql.Time.valueOf", i);
        case "DATE":
            return m("java.sql.Date.valueOf", i);
        case "TIMESTAMP":
            return m("java.sql.Timestamp.valueOf", i);
        case "BYTES":
            // "[B", not "byte[]";
            return "ja.getString(" + i + ").getBytes()";
        case "UUID":
            return m("java.util.UUID.fromString", i);
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "ja.getString(" + i + ")";
        case "BLOB":
            // "java.sql.Blob";
            throw DbException.throwInternalError("type=" + type); // return java.sql.Blob.class.getName(); // TODO
        case "CLOB":
            // "java.sql.Clob";
            throw DbException.throwInternalError("type=" + type); // return java.sql.Clob.class.getName(); // TODO
        case "DOUBLE":
            return m("Double.valueOf", i);
        case "FLOAT":
            return m("Float.valueOf", i);
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
            return "ja.getJsonObject(" + i + ").mapTo(" + type0 + ".class)";
        // throw DbException.throwInternalError("type=" + type);
        }
    }
}
