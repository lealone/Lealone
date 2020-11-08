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
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.service.Service;
import org.lealone.db.service.ServiceExecutor;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.CreateTableData;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
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

    @Override
    public boolean isReplicationStatement() {
        return true;
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
    public int update() {
        session.getUser().checkAdmin();
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.SERVICE, session);
        if (lock == null)
            return -1;

        if (schema.findService(session, serviceName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.SERVICE_ALREADY_EXISTS_1, serviceName);
        }
        int id = getObjectId();
        Service service = new Service(schema, id, serviceName, sql, getExecutorFullName());
        service.setImplementBy(implementBy);
        service.setPackageName(packageName);
        service.setComment(comment);
        schema.add(session, service, lock);
        // 数据库在启动阶段执行CREATE SERVICE语句时不用再生成代码
        if (genCode && !session.getDatabase().isStarting())
            genCode();
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

        StringBuilder psBuff = new StringBuilder();
        StringBuilder psInitBuff = new StringBuilder();

        TreeSet<String> importSet = new TreeSet<>();
        importSet.add("org.lealone.client.ClientServiceProxy");
        importSet.add("java.sql.*");

        String serviceName = toClassName(this.serviceName);

        buff.append("public interface ").append(serviceName).append(" {\r\n");
        buff.append("\r\n");
        buff.append("    static ").append(serviceName).append(" create(String url) {\r\n");
        buff.append("        if (new org.lealone.db.ConnectionInfo(url).isEmbedded())\r\n");
        buff.append("            return new ").append(getServiceImplementClassName()).append("();\r\n");
        buff.append("        else;\r\n");
        buff.append("            return new ServiceProxy(url);\r\n");
        buff.append("    }\r\n");

        int methodIndex = 0;
        for (CreateTable m : serviceMethods) {
            methodIndex++;
            String psVarName = "ps" + methodIndex;
            CreateTableData data = m.data;
            buff.append("\r\n");
            proxyMethodsBuff.append("\r\n");

            psBuff.append("        private final PreparedStatement ").append(psVarName).append(";\r\n");
            psInitBuff.append("            ").append(psVarName)
                    .append(" = ClientServiceProxy.prepareStatement(url, \"EXECUTE SERVICE ").append(this.serviceName)
                    .append(" ").append(data.tableName).append("(");
            for (int i = 0, size = data.columns.size() - 1; i < size; i++) {
                if (i != 0) {
                    psInitBuff.append(", ");
                }
                psInitBuff.append("?");
            }
            psInitBuff.append(")\");\r\n");

            Column returnColumn = data.columns.get(data.columns.size() - 1);
            String returnType = getTypeName(returnColumn, importSet);
            String methodName = toMethodName(data.tableName);
            buff.append("    ").append(returnType).append(" ").append(methodName).append("(");

            proxyMethodsBuff.append("        @Override\r\n");
            proxyMethodsBuff.append("        public ").append(returnType).append(" ").append(methodName).append("(");

            StringBuilder argsBuff = new StringBuilder();

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
                if (c.getTable() != null || cType.toUpperCase().equals("UUID")) {
                    importSet.add("org.lealone.orm.json.JsonObject");
                    argsBuff.append("                ").append(psVarName).append(".setString(").append(i + 1)
                            .append(", JsonObject.mapFrom(").append(cName).append(").encode());\r\n");
                } else {
                    argsBuff.append("                ").append(psVarName).append(".")
                            .append(getPreparedStatementSetterMethodName(cType)).append("(").append(i + 1).append(", ")
                            .append(cName).append(");\r\n");
                }
            }
            buff.append(");\r\n");

            proxyMethodsBuff.append(") {\r\n");
            proxyMethodsBuff.append("            try {\r\n");
            proxyMethodsBuff.append(argsBuff);
            if (returnType.equalsIgnoreCase("void")) {
                proxyMethodsBuff.append("                ").append(psVarName).append(".executeUpdate();\r\n");
            } else {
                proxyMethodsBuff.append("                ResultSet rs = ").append(psVarName)
                        .append(".executeQuery();\r\n");
                proxyMethodsBuff.append("                rs.next();\r\n");

                if (returnColumn.getTable() != null) {
                    importSet.add("org.lealone.orm.json.JsonObject");
                    proxyMethodsBuff.append("                JsonObject jo = new JsonObject(rs.getString(1));\r\n");
                    proxyMethodsBuff.append("                rs.close();\r\n");
                    proxyMethodsBuff.append("                return jo.mapTo(").append(returnType)
                            .append(".class);\r\n");
                } else {
                    proxyMethodsBuff.append("                ").append(returnType).append(" ret = rs.")
                            .append(getResultSetReturnMethodName(returnType)).append("(1);\r\n");
                    proxyMethodsBuff.append("                rs.close();\r\n");
                    proxyMethodsBuff.append("                return ret;\r\n");
                }
            }
            proxyMethodsBuff.append("            } catch (Throwable e) {\r\n");
            proxyMethodsBuff.append("                throw ClientServiceProxy.failed(\"").append(this.serviceName)
                    .append('.').append(data.tableName).append("\", e);\r\n");
            proxyMethodsBuff.append("            }\r\n");
            proxyMethodsBuff.append("        }\r\n");
        }

        // 生成Service Proxy类
        buff.append("\r\n");
        buff.append("    static class ServiceProxy implements ").append(serviceName).append(" {\r\n");
        buff.append("\r\n");
        buff.append(psBuff);
        buff.append("\r\n");
        buff.append("        private ServiceProxy(String url) {\r\n");
        buff.append(psInitBuff);
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

        writeFile(codePath, packageName, serviceName, ibuff, buff);
    }

    private String getServiceImplementClassName() {
        return implementBy;
    }

    private abstract class ServiceExecutorMethodGenerator {
        public void genCode(StringBuilder buff, TreeSet<String> importSet, String method) {
            genCode(buff, importSet, method, "");
        }

        public void genCode(StringBuilder buff, TreeSet<String> importSet, String method, String varInit) {
            buff.append("\r\n");
            buff.append("    @Override\r\n");
            buff.append("    public ").append(method).append(" {\r\n");
            buff.append(varInit);
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
                    if (varInit.length() > 0)
                        buff.append("            ja = new JsonArray(json);\r\n");
                    for (int i = 0; i < size; i++) {
                        if (i != 0) {
                            argsBuff.append(", ");
                        }
                        Column c = data.columns.get(i);
                        String cType = getTypeName(c, importSet);
                        String cName = "p_" + toFieldName(c.getName()) + index;

                        buff.append("            ").append(cType).append(" ").append(cName).append(" = ");
                        genVarInitCode(buff, importSet, c, cType, i);
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
                    genReturnCode(buff, importSet, returnColumn, returnType, resultVarName);
                } else {
                    buff.append("            break;\r\n");
                }
            }
            buff.append("        default:\r\n");
            buff.append("            throw new RuntimeException(\"no method: \" + methodName);\r\n");
            buff.append("        }\r\n");
            if (hasNoReturnValueMethods)
                buff.append("        return ").append(getReturnType()).append(";\r\n");
            buff.append("    }\r\n");
        }

        protected abstract void genVarInitCode(StringBuilder buff, TreeSet<String> importSet, Column c, String cType,
                int cIndex);

        protected void genReturnCode(StringBuilder buff, TreeSet<String> importSet, Column returnColumn,
                String returnType, String resultVarName) {
            buff.append("            if (").append(resultVarName).append(" == null)\r\n");
            buff.append("                return null;\r\n");
            if (returnColumn.getTable() != null) {
                importSet.add("org.lealone.orm.json.JsonObject");
                buff.append("            return JsonObject.mapFrom(").append(resultVarName).append(").encode();\r\n");
            } else if (!returnType.equalsIgnoreCase("string")) {
                buff.append("            return ").append(resultVarName).append(".toString();\r\n");
            } else {
                buff.append("            return ").append(resultVarName).append(";\r\n");
            }
        }

        protected String getReturnType() {
            return "NO_RETURN_VALUE";
        }
    }

    private class ValueServiceExecutorMethodGenerator extends ServiceExecutorMethodGenerator {
        @Override
        protected void genVarInitCode(StringBuilder buff, TreeSet<String> importSet, Column c, String cType,
                int cIndex) {
            if (c.getTable() != null) {
                importSet.add("org.lealone.orm.json.JsonObject");
                buff.append(" new JsonObject(").append("methodArgs[").append(cIndex).append("].getString()).mapTo(")
                        .append(cType).append(".class);\r\n");
            } else {
                buff.append("methodArgs[").append(cIndex).append("].").append(getValueMethodName(cType))
                        .append("();\r\n");
            }
        }

        @Override
        protected void genReturnCode(StringBuilder buff, TreeSet<String> importSet, Column returnColumn,
                String returnType, String resultVarName) {
            buff.append("            if (").append(resultVarName).append(" == null)\r\n");
            buff.append("                return ValueNull.INSTANCE;\r\n");
            if (returnColumn.getTable() != null) {
                importSet.add("org.lealone.orm.json.JsonObject");
                buff.append("            return ValueString.get(JsonObject.mapFrom(").append(resultVarName)
                        .append(").encode());\r\n");
            } else {
                buff.append("            return ").append(getReturnMethodName(returnType)).append("(")
                        .append(resultVarName).append(")").append(";\r\n");
            }
        }

        @Override
        protected String getReturnType() {
            return "ValueNull.INSTANCE";
        }
    }

    private class MapServiceExecutorMethodGenerator extends ServiceExecutorMethodGenerator {
        @Override
        protected void genVarInitCode(StringBuilder buff, TreeSet<String> importSet, Column c, String cType,
                int cIndex) {
            if (c.getTable() != null) {
                importSet.add("org.lealone.orm.json.JsonObject");
                buff.append(" new JsonObject(").append("methodArgs.get(\"").append(c.getName()).append("\")).mapTo(")
                        .append(cType).append(".class);\r\n");
            } else {
                switch (cType.toUpperCase()) {
                case "STRING":
                    buff.append("methodArgs.get(\"").append(c.getName()).append("\");\r\n");
                    break;
                case "BYTE[]":
                    buff.append("methodArgs.get(\"").append(c.getName()).append("\").getBytes();\r\n");
                    break;
                default:
                    buff.append(getMapMethodName(cType)).append("(").append("methodArgs.get(\"").append(c.getName())
                            .append("\"));\r\n");
                }
            }
        }
    }

    private class JsonServiceExecutorMethodGenerator extends ServiceExecutorMethodGenerator {
        @Override
        protected void genVarInitCode(StringBuilder buff, TreeSet<String> importSet, Column c, String cType,
                int cIndex) {
            buff.append(getJsonArrayMethodName(cType, cIndex)).append(";\r\n");
        }
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
        String className = getExecutorSimpleName();

        buff.append("public class ").append(className).append(" implements ServiceExecutor {\r\n");
        buff.append("\r\n");
        buff.append("    private final ").append(serviceImplementClassName).append(" s = new ")
                .append(serviceImplementClassName).append("();\r\n");

        // 生成默认构造函数
        // buff.append("\r\n");
        // buff.append(" public ").append(className).append("() {\r\n");
        // buff.append(" }\r\n");
        // buff.append("\r\n");

        // 生成public Value executeService(String methodName, Value[] methodArgs)方法
        importSet.add("org.lealone.db.value.*");
        new ValueServiceExecutorMethodGenerator().genCode(buff, importSet,
                "Value executeService(String methodName, Value[] methodArgs)");

        // 生成public String executeService(String methodName, Map<String, String> methodArgs)方法
        importSet.add(Map.class.getName());
        new MapServiceExecutorMethodGenerator().genCode(buff, importSet,
                "String executeService(String methodName, Map<String, String> methodArgs)");

        // 生成public String executeService(String methodName, String json)方法
        // 提前看一下是否用到JsonArray
        String varInit = "";
        for (CreateTable m : serviceMethods) {
            if (m.data.columns.size() - 1 > 0) {
                importSet.add("org.lealone.orm.json.JsonArray");
                varInit = "        JsonArray ja = null;\r\n";
                break;
            }
        }
        new JsonServiceExecutorMethodGenerator().genCode(buff, importSet,
                "String executeService(String methodName, String json)", varInit);

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
    }

    private String getExecutorPackageName() {
        return packageName + ".executor";
    }

    private String getExecutorFullName() {
        return getExecutorPackageName() + "." + getExecutorSimpleName();
    }

    private String getExecutorSimpleName() {
        return toClassName(serviceName) + "Executor";
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
            switch (c.getType()) {
            case Value.BYTES:
                cType = "byte[]";
                break;
            case Value.UUID:
                cType = UUID.class.getName();
                break;
            default:
                cType = DataType.getTypeClassName(c.getType());
            }
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

    private static String getReturnMethodName(String type) {
        type = type.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return "ValueBoolean.get";
        case "BYTE":
            return "ValueByte.get";
        case "SHORT":
            return "ValueShort.get";
        case "INTEGER":
            return "ValueInt.get";
        case "LONG":
            return "ValueLong.get";
        case "DECIMAL":
            return "ValueDecimal.get";
        case "TIME":
            return "ValueTime.get";
        case "DATE":
            return "ValueDate.get";
        case "TIMESTAMP":
            return "ValueTimestamp.get";
        case "BYTE[]":
            return "ValueBytes.get";
        case "UUID":
            return "ValueUuid.get";
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "ValueString.get";
        case "DOUBLE":
            return "ValueDouble.get";
        case "FLOAT":
            return "ValueFloat.get";
        case "NULL":
            return "ValueNull.INSTANCE";
        case "UNKNOWN": // anything
        case "JAVA_OBJECT":
            return "ValueShort.get";
        case "BLOB":
            return "ValueShort.get";
        case "CLOB":
            return "ValueShort.get";
        case "ARRAY":
            return "ValueShort.get";
        case "RESULT_SET":
            return "ValueResultSet.get";
        }
        return "ValueShort.get";
    }

    private static String m(String str, int i) {
        return str + "(ja.getValue(" + i + ").toString())";
    }

    // 根据具体类型调用合适的JsonArray方法
    private static String getJsonArrayMethodName(String type0, int i) {
        String type = type0.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return m("Boolean.valueOf", i);
        case "BYTE":
            return m("Byte.valueOf", i);
        case "SHORT":
            return m("Short.valueOf", i);
        case "INTEGER":
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
        case "BYTE[]":
            return "ja.getString(" + i + ").getBytes()";
        case "UUID":
            return m("java.util.UUID.fromString", i);
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "ja.getString(" + i + ")";
        case "DOUBLE":
            return m("Double.valueOf", i);
        case "FLOAT":
            return m("Float.valueOf", i);
        case "NULL":
            return null;
        case "UNKNOWN": // anything
        case "JAVA_OBJECT":
            return "ja.getJsonObject(" + i + ")";
        case "BLOB":
            type0 = "java.sql.Blob";
            break;
        case "CLOB":
            type0 = "java.sql.Clob";
            break;
        case "ARRAY":
            type0 = "java.sql.Array";
            break;
        case "RESULT_SET":
            type0 = "java.sql.ResultSet";
            break;
        }
        return "ja.getJsonObject(" + i + ").mapTo(" + type0 + ".class)";
    }

    // 根据具体类型调用合适的Map方法
    private static String getMapMethodName(String type0) {
        String type = type0.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return "Boolean.valueOf";
        case "BYTE":
            return "Byte.valueOf";
        case "SHORT":
            return "Short.valueOf";
        case "INTEGER":
            return "Integer.valueOf";
        case "LONG":
            return "Long.valueOf";
        case "BIGDECIMAL":
            return "new java.math.BigDecimal";
        case "TIME":
            return "java.sql.Time.valueOf";
        case "DATE":
            return "java.sql.Date.valueOf";
        case "TIMESTAMP":
            return "java.sql.Timestamp.valueOf";
        case "BYTE[]":
            return "s.getBytes()";
        case "UUID":
            return "java.util.UUID.fromString";
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "";
        case "DOUBLE":
            return "Double.valueOf";
        case "FLOAT":
            return "Float.valueOf";
        case "NULL":
            return null;
        case "UNKNOWN": // anything
        case "JAVA_OBJECT":
            return "";
        case "BLOB":
            return "new org.lealone.db.value.ReadonlyBlob";
        case "CLOB":
            return "new org.lealone.db.value.ReadonlyClob";
        case "ARRAY":
            return "new org.lealone.db.value.ReadonlyArray";
        case "RESULT_SET":
            type0 = "java.sql.ResultSet";
            break;
        }
        return "";
    }

    // 根据具体类型调用合适的Value方法
    private static String getValueMethodName(String type) {
        type = type.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return "getBoolean";
        case "BYTE":
            return "getByte";
        case "SHORT":
            return "getShort";
        case "INTEGER":
            return "getInt";
        case "LONG":
            return "getLong";
        case "BIGDECIMAL":
            return "getBigDecimal";
        case "TIME":
            return "getTime";
        case "DATE":
            return "getDate";
        case "TIMESTAMP":
            return "getTimestamp";
        case "BYTE[]":
            return "getBytes";
        case "UUID":
            return "getUuid";
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "getString";
        case "DOUBLE":
            return "getDouble";
        case "FLOAT":
            return "getFloat";
        case "NULL":
            return null;
        case "UNKNOWN": // anything
        case "JAVA_OBJECT":
            return "getObject";
        case "BLOB":
            return "getBlob";
        case "CLOB":
            return "getClob";
        case "ARRAY":
            return "getArray";
        case "RESULT_SET":
            return "getResultSet";
        }
        return "getObject";
    } // 根据具体类型调用合适的Value方法

    private static String getResultSetReturnMethodName(String type) {
        type = type.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return "getBoolean";
        case "BYTE":
            return "getByte";
        case "SHORT":
            return "getShort";
        case "INTEGER":
            return "getInt";
        case "LONG":
            return "getLong";
        case "DECIMAL":
            return "getBigDecimal";
        case "TIME":
            return "getTime";
        case "DATE":
            return "getDate";
        case "TIMESTAMP":
            return "getTimestamp";
        case "BYTE[]":
            return "getBytes";
        case "UUID":
            return "getUuid"; // TODO
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "getString";
        case "DOUBLE":
            return "getDouble";
        case "FLOAT":
            return "getFloat";
        case "NULL":
            return null;
        case "UNKNOWN": // anything
        case "JAVA_OBJECT":
            return "getObject";
        case "BLOB":
            return "getBlob";
        case "CLOB":
            return "getClob";
        case "ARRAY":
            return "getArray";
        case "RESULT_SET":
            return "getResultSet"; // TODO
        }
        return "getObject";
    }

    private static String getPreparedStatementSetterMethodName(String type) {
        type = type.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return "setBoolean";
        case "BYTE":
            return "setByte";
        case "SHORT":
            return "setShort";
        case "INTEGER":
            return "setInt";
        case "LONG":
            return "setLong";
        case "DECIMAL":
            return "setBigDecimal";
        case "TIME":
            return "setTime";
        case "DATE":
            return "setDate";
        case "TIMESTAMP":
            return "setTimestamp";
        case "BYTE[]":
            return "setBytes";
        case "UUID":
            return "setUuid"; // TODO
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "setString";
        case "DOUBLE":
            return "setDouble";
        case "FLOAT":
            return "setFloat";
        case "NULL":
            return null;
        case "UNKNOWN": // anything
        case "JAVA_OBJECT":
            return "setObject";
        case "BLOB":
            return "setBlob";
        case "CLOB":
            return "setClob";
        case "ARRAY":
            return "setArray";
        case "RESULT_SET":
            return "setResultSet"; // TODO
        }
        return "setObject";
    }
}
