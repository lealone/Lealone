/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.CamelCaseHelper;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.db.service.Service;
import com.lealone.db.service.ServiceCodeGeneratorBase;
import com.lealone.db.service.ServiceExecutor;
import com.lealone.db.service.ServiceSetting;
import com.lealone.db.table.Column;
import com.lealone.db.table.Column.ListColumn;
import com.lealone.db.table.Column.MapColumn;
import com.lealone.db.table.Column.SetColumn;
import com.lealone.db.table.CreateTableData;
import com.lealone.db.value.ValueUuid;
import com.lealone.orm.ModelCodeGenerator;
import com.lealone.sql.ddl.CreateTable;

public class JavaServiceCodeGenerator extends ServiceCodeGeneratorBase {

    private List<CreateTable> serviceMethods;
    private CaseInsensitiveMap<String> serviceParameters;
    private String codePath;
    private boolean genCode;
    private String serviceName;
    private String packageName;
    private String implementBy;

    public JavaServiceCodeGenerator() {
        super("default_service_code_generator");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Service service, List<?> serviceMethods,
            CaseInsensitiveMap<String> serviceParameters, String codePath, boolean genCode) {
        this.serviceMethods = (List<CreateTable>) serviceMethods;
        this.serviceParameters = serviceParameters;
        this.codePath = codePath;
        this.genCode = genCode;
        serviceName = service.getName();
        packageName = service.getPackageName();
        implementBy = service.getImplementBy();
    }

    private static String toClassName(String n) {
        return ModelCodeGenerator.toClassName(n);
    }

    private static String toMethodName(String n) {
        return CamelCaseHelper.toCamelFromUnderscore(n);
    }

    private static String toFieldName(String n) {
        return CamelCaseHelper.toCamelFromUnderscore(n);
    }

    @Override
    public void genServiceInterfaceCode() {
        // PreparedStatement字段声明
        StringBuilder psBuff = new StringBuilder();
        // PreparedStatement字段初始化
        StringBuilder psInitBuff = new StringBuilder();

        TreeSet<String> importSet = new TreeSet<>();
        importSet.add("com.lealone.client.ClientServiceProxy");
        importSet.add("java.sql.*");

        // 生成方法签名和方法体的代码
        int methodSize = serviceMethods.size();
        ArrayList<StringBuilder> methodSignatureList = new ArrayList<>(methodSize);
        ArrayList<StringBuilder> proxyMethodBodyList = new ArrayList<>(methodSize);

        for (int methodIndex = 0; methodIndex < methodSize; methodIndex++) {
            StringBuilder methodSignatureBuff = new StringBuilder();
            StringBuilder proxyMethodBodyBuff = new StringBuilder();
            String psVarName = "ps" + (methodIndex + 1);
            CreateTableData data = serviceMethods.get(methodIndex).getCreateTableData();

            psBuff.append("        private final PreparedStatement ").append(psVarName).append(";\r\n");
            psInitBuff.append("            ").append(psVarName)
                    .append(" = ClientServiceProxy.prepareStatement(url, \"EXECUTE SERVICE ")
                    .append(serviceName).append(" ").append(data.tableName).append("(");

            // 最后一列是返回类型所以要减一
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
            methodSignatureBuff.append(returnType).append(" ").append(methodName).append("(");

            proxyMethodBodyBuff.append("            try {\r\n");

            for (int i = 0, size = data.columns.size() - 1; i < size; i++) {
                if (i != 0) {
                    methodSignatureBuff.append(", ");
                }
                Column c = data.columns.get(i);
                String cType = getTypeName(c, importSet);
                String cName = toFieldName(c.getName());
                methodSignatureBuff.append(cType).append(" ").append(cName);
                proxyMethodBodyBuff.append("                ").append(psVarName).append(".");
                if (c.getTable() != null) {
                    proxyMethodBodyBuff.append("setString(").append(i + 1).append(", ").append(cName)
                            .append(".encode());\r\n");
                } else {
                    proxyMethodBodyBuff.append(getPreparedStatementSetterMethodName(cType)).append("(")
                            .append(i + 1).append(", ");
                    if (cType.toUpperCase().equals("UUID")) {
                        importSet.add(UUID.class.getName());
                        importSet.add(ValueUuid.class.getName());
                        proxyMethodBodyBuff.append("ValueUuid.get(").append(cName)
                                .append(").getBytes()");
                    } else {
                        proxyMethodBodyBuff.append(cName);
                    }
                    proxyMethodBodyBuff.append(");\r\n");
                }
            }
            methodSignatureBuff.append(")");
            methodSignatureList.add(methodSignatureBuff);

            if (returnType.equals("void")) {
                proxyMethodBodyBuff.append("                ").append(psVarName)
                        .append(".executeUpdate();\r\n");
            } else {
                proxyMethodBodyBuff.append("                ResultSet rs = ").append(psVarName)
                        .append(".executeQuery();\r\n");
                proxyMethodBodyBuff.append("                rs.next();\r\n");

                if (returnColumn.isCollectionType()) {
                    proxyMethodBodyBuff.append("                @SuppressWarnings(\"unchecked\")\r\n");
                    proxyMethodBodyBuff.append("                ").append(returnType).append(" ret = (")
                            .append(returnType).append(")rs.getObject(1);\r\n");
                    proxyMethodBodyBuff.append("                rs.close();\r\n");
                    proxyMethodBodyBuff.append("                return ret;\r\n");
                } else if (returnColumn.getTable() != null) {
                    proxyMethodBodyBuff.append("                String ret = rs.getString(1);\r\n");
                    proxyMethodBodyBuff.append("                rs.close();\r\n");
                    proxyMethodBodyBuff.append("                return ").append(returnType)
                            .append(".decode(ret);\r\n");
                } else {
                    proxyMethodBodyBuff.append("                ").append(returnType).append(" ret = ");
                    if (returnType.toUpperCase().equals("UUID")) {
                        importSet.add(UUID.class.getName());
                        importSet.add(ValueUuid.class.getName());
                        proxyMethodBodyBuff.append("ValueUuid.get(rs.")
                                .append(getResultSetReturnMethodName(returnType))
                                .append("(1)).getUuid();\r\n");
                    } else {
                        proxyMethodBodyBuff.append("rs.")
                                .append(getResultSetReturnMethodName(returnType)).append("(1);\r\n");
                    }
                    proxyMethodBodyBuff.append("                rs.close();\r\n");
                    proxyMethodBodyBuff.append("                return ret;\r\n");
                }
            }
            proxyMethodBodyBuff.append("            } catch (Throwable e) {\r\n");
            proxyMethodBodyBuff.append("                throw ClientServiceProxy.failed(\"")
                    .append(serviceName).append('.').append(data.tableName).append("\", e);\r\n");
            proxyMethodBodyBuff.append("            }\r\n");

            proxyMethodBodyList.add(proxyMethodBodyBuff);
        }

        // 生成package和import代码
        StringBuilder buff = new StringBuilder();
        buff.append("package ").append(packageName).append(";\r\n");
        buff.append("\r\n");
        for (String i : importSet) {
            buff.append("import ").append(i).append(";\r\n");
        }
        buff.append("\r\n");

        // 接口注释
        buff.append("/**\r\n");
        buff.append(" * Service interface for '").append(serviceName.toLowerCase()).append("'.\r\n");
        buff.append(" *\r\n");
        buff.append(" * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.\r\n");
        buff.append(" */\r\n");

        // 服务接口
        String serviceInterfaceName = toClassName(serviceName);
        buff.append("public interface ").append(serviceInterfaceName).append(" {\r\n");
        buff.append("\r\n");

        // 生成服务接口方法
        for (StringBuilder m : methodSignatureList) {
            buff.append("    ").append(m).append(";\r\n");
            buff.append("\r\n");
        }

        String createMethodName = getParameterValue(ServiceSetting.CREATE_METHOD_NAME, "create");

        // 生成两个static create方法
        buff.append("    static ").append(serviceInterfaceName).append(" ").append(createMethodName)
                .append("() {\r\n");
        buff.append("        return ").append(createMethodName).append("(null);\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");
        buff.append("    static ").append(serviceInterfaceName).append(" ").append(createMethodName)
                .append("(String url) {\r\n");
        buff.append("        if (url == null)\r\n");
        buff.append("            url = ClientServiceProxy.getUrl();\r\n");
        buff.append("\r\n");
        buff.append("        if (ClientServiceProxy.isEmbedded(url))\r\n");
        buff.append("            return new ").append(implementBy).append("();\r\n");
        buff.append("        else\r\n");
        buff.append("            return new ServiceProxy(url);\r\n");
        buff.append("    }\r\n");

        // 生成Service Proxy类
        buff.append("\r\n");
        buff.append("    static class ServiceProxy implements ").append(serviceInterfaceName)
                .append(" {\r\n");
        buff.append("\r\n");
        buff.append(psBuff);
        buff.append("\r\n");
        buff.append("        private ServiceProxy(String url) {\r\n");
        buff.append(psInitBuff);
        buff.append("        }\r\n");
        for (int i = 0; i < methodSize; i++) {
            buff.append("\r\n");
            buff.append("        @Override\r\n");
            buff.append("        public ").append(methodSignatureList.get(i)).append(" {\r\n");
            buff.append(proxyMethodBodyList.get(i));
            buff.append("        }\r\n");
        }
        buff.append("    }\r\n");
        buff.append("}\r\n");

        ModelCodeGenerator.writeFile(codePath, packageName, serviceInterfaceName, buff);
    }

    private boolean isServiceImplementClassExists() {
        String path = codePath;
        path = path.replace('/', File.separatorChar);
        if (!path.endsWith(File.separator))
            path = path + File.separator;
        String srcFile = path + implementBy.replace('.', File.separatorChar) + ".java";
        return new File(srcFile).exists();
    }

    private String getParameterValue(ServiceSetting key, String defaultValue) {
        String v = serviceParameters == null ? null : serviceParameters.get(key.name());
        return v == null ? defaultValue : v.trim();
    }

    @Override
    public void genServiceImplementClassCode() {
        if (isServiceImplementClassExists()) {
            return;
        }
        TreeSet<String> importSet = new TreeSet<>();

        // 生成方法签名和方法体的代码
        int methodSize = serviceMethods.size();
        ArrayList<StringBuilder> methodSignatureList = new ArrayList<>(methodSize);
        ArrayList<String> methodReturnTypeList = new ArrayList<>(methodSize);

        for (int methodIndex = 0; methodIndex < methodSize; methodIndex++) {
            StringBuilder methodSignatureBuff = new StringBuilder();
            CreateTableData data = serviceMethods.get(methodIndex).getCreateTableData();

            Column returnColumn = data.columns.get(data.columns.size() - 1);
            String returnType = getTypeName(returnColumn, importSet);
            String methodName = toMethodName(data.tableName);
            methodSignatureBuff.append(returnType).append(" ").append(methodName).append("(");
            methodReturnTypeList.add(returnType);

            for (int i = 0, size = data.columns.size() - 1; i < size; i++) {
                if (i != 0) {
                    methodSignatureBuff.append(", ");
                }
                Column c = data.columns.get(i);
                String cType = getTypeName(c, importSet);
                String cName = toFieldName(c.getName());
                methodSignatureBuff.append(cType).append(" ").append(cName);
            }
            methodSignatureBuff.append(")");
            methodSignatureList.add(methodSignatureBuff);
        }

        String serviceInterfaceName = toClassName(serviceName);
        int pos = implementBy.lastIndexOf('.');
        String implementPackageName = implementBy.substring(0, pos);
        String implementClassName = implementBy.substring(pos + 1);
        if (!implementPackageName.equals(packageName)) {
            importSet.add(packageName + "." + serviceInterfaceName);
        }
        // 生成package和import代码
        StringBuilder buff = new StringBuilder();
        buff.append("package ").append(implementPackageName).append(";\r\n");
        if (importSet.size() > 0) {
            buff.append("\r\n");
            for (String i : importSet) {
                buff.append("import ").append(i).append(";\r\n");
            }
        }
        buff.append("\r\n");

        // 生成Service实现类的骨架代码
        buff.append("public class ").append(implementClassName);
        if (genCode)
            buff.append(" implements ").append(serviceInterfaceName);
        buff.append(" {\r\n");
        for (int i = 0; i < methodSize; i++) {
            buff.append("\r\n");
            if (genCode)
                buff.append("    @Override\r\n");
            buff.append("    public ").append(methodSignatureList.get(i)).append(" {\r\n");
            if (!methodReturnTypeList.get(i).equals("void"))
                buff.append("        return null;\r\n");
            buff.append("    }\r\n");
        }
        buff.append("}\r\n");

        ModelCodeGenerator.writeFile(codePath, implementPackageName, implementClassName, buff);
    }

    private abstract class ServiceExecutorMethodGenerator {

        public StringBuilder genCode(TreeSet<String> importSet, String method) {
            return genCode(importSet, method, "");
        }

        public StringBuilder genCode(TreeSet<String> importSet, String method, String varInit) {
            StringBuilder buff = new StringBuilder();
            buff.append("\r\n");
            buff.append("    @Override\r\n");
            buff.append("    public ").append(method).append(" {\r\n");
            buff.append(varInit);
            buff.append("        switch (methodName) {\r\n");

            int index = 0;
            for (CreateTable m : serviceMethods) {
                index++;
                // switch语句不同case代码块的本地变量名不能相同
                String resultVarName = "result" + index;
                CreateTableData data = m.getCreateTableData();

                Column returnColumn = data.columns.get(data.columns.size() - 1);
                String returnType = getTypeName(returnColumn, importSet);
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
                        String cName = "p_" + toFieldName(c.getName()) + "_" + index;

                        buff.append("            ").append(cType).append(" ").append(cName)
                                .append(" = ");
                        genVarInitCode(buff, importSet, c, cType, i);
                        argsBuff.append(cName);
                    }
                }
                boolean isVoid = returnType.equals("void");
                buff.append("            ");
                if (!isVoid) {
                    if (createResultVar())
                        buff.append(returnType).append(" ").append(resultVarName).append(" = ");
                    else
                        buff.append("return ");
                }
                buff.append("si.").append(methodName).append("(").append(argsBuff).append(");\r\n");
                if (!isVoid) {
                    if (createResultVar())
                        genReturnCode(buff, importSet, returnColumn, returnType, resultVarName);
                } else {
                    buff.append("            return ").append(getReturnType()).append(";\r\n");
                }
            }
            buff.append("        default:\r\n");
            buff.append("            throw noMethodException(methodName);\r\n");
            buff.append("        }\r\n");
            buff.append("    }\r\n");
            return buff;
        }

        protected abstract void genVarInitCode(StringBuilder buff, TreeSet<String> importSet, Column c,
                String cType, int cIndex);

        protected void genReturnCode(StringBuilder buff, TreeSet<String> importSet, Column returnColumn,
                String returnType, String resultVarName) {
            buff.append("            return ").append(resultVarName).append(";\r\n");
        }

        protected String getReturnType() {
            return "NO_RETURN_VALUE";
        }

        protected boolean createResultVar() {
            return false;
        }
    }

    private class ValueServiceExecutorMethodGenerator extends ServiceExecutorMethodGenerator {
        @Override
        protected void genVarInitCode(StringBuilder buff, TreeSet<String> importSet, Column c,
                String cType, int cIndex) {
            if (c.isCollectionType()) {
                buff.append("methodArgs[").append(cIndex).append("].getCollection();\r\n");
            } else if (c.getTable() != null) {
                buff.append(cType).append(".decode(").append("methodArgs[").append(cIndex)
                        .append("].getString());\r\n");
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
                buff.append("            return ValueString.get(").append(resultVarName)
                        .append(".encode());\r\n");
            } else {
                Column c = returnColumn;
                if (c instanceof ListColumn) {
                    ListColumn lc = (ListColumn) c;
                    buff.append("            return ValueList.get(")
                            .append(getTypeName(lc.element, importSet)).append(".class, ")
                            .append(resultVarName).append(");\r\n");
                } else if (c instanceof SetColumn) {
                    SetColumn sc = (SetColumn) c;
                    buff.append("            return ValueSet.get(")
                            .append(getTypeName(sc.element, importSet)).append(".class, ")
                            .append(resultVarName).append(");\r\n");
                } else if (c instanceof MapColumn) {
                    MapColumn mc = (MapColumn) c;
                    buff.append("            return ValueMap.get(")
                            .append(getTypeName(mc.key, importSet)).append(".class, ")
                            .append(getTypeName(mc.value, importSet)).append(".class, ")
                            .append(resultVarName).append(");\r\n");
                } else {
                    buff.append("            return ").append(getReturnMethodName(returnType))
                            .append("(").append(resultVarName).append(")").append(";\r\n");
                }
            }
        }

        @Override
        protected String getReturnType() {
            return "ValueNull.INSTANCE";
        }

        @Override
        protected boolean createResultVar() {
            return true;
        }
    }

    private class MapServiceExecutorMethodGenerator extends ServiceExecutorMethodGenerator {
        @Override
        protected void genVarInitCode(StringBuilder buff, TreeSet<String> importSet, Column c,
                String cType, int cIndex) {
            String cName = c.getName();
            String methodName;
            if (c.getTable() != null) {
                buff.append(cType).append(".decode(").append("toString(\"").append(cName)
                        .append("\", methodArgs));\r\n");
            } else {
                if (c instanceof ListColumn) {
                    methodName = "toList";
                } else if (c instanceof SetColumn) {
                    methodName = "toSet";
                } else if (c instanceof MapColumn) {
                    methodName = "toMap";
                } else {
                    methodName = getMapMethodName(cType);
                }
                buff.append(methodName).append("(\"").append(cName).append("\", methodArgs);\r\n");
            }
        }
    }

    private class JsonServiceExecutorMethodGenerator extends ServiceExecutorMethodGenerator {
        @Override
        protected void genVarInitCode(StringBuilder buff, TreeSet<String> importSet, Column c,
                String cType, int cIndex) {
            buff.append(getJsonArrayMethodName(c, cType, cIndex, importSet)).append(";\r\n");
        }
    }

    @Override
    public StringBuilder genServiceExecutorCode(boolean writeFile) {
        TreeSet<String> importSet = new TreeSet<>();
        importSet.add(ServiceExecutor.class.getName());

        // 生成public Value executeService(String methodName, Value[] methodArgs)方法
        importSet.add("com.lealone.db.value.*");
        StringBuilder buffValueMethod = new ValueServiceExecutorMethodGenerator().genCode(importSet,
                "Value executeService(String methodName, Value[] methodArgs)");

        boolean generateMapExecutorMethod = Boolean
                .parseBoolean(getParameterValue(ServiceSetting.GENERATE_MAP_EXECUTOR_METHOD, "true"));
        boolean generateJsonExecutorMethod = Boolean
                .parseBoolean(getParameterValue(ServiceSetting.GENERATE_JSON_EXECUTOR_METHOD, "true"));
        StringBuilder buffMapMethod = null;
        StringBuilder buffJsonMethod = null;

        // 生成public String executeService(String methodName, Map<String, Object> methodArgs)方法
        if (generateMapExecutorMethod) {
            importSet.add(Map.class.getName());
            buffMapMethod = new MapServiceExecutorMethodGenerator().genCode(importSet,
                    "Object executeService(String methodName, Map<String, Object> methodArgs)");
        }

        // 生成public String executeService(String methodName, String json)方法
        if (generateJsonExecutorMethod) {
            // 提前看一下是否用到JsonArray
            String varInit = "";
            for (CreateTable m : serviceMethods) {
                if (m.getCreateTableData().columns.size() - 1 > 0) {
                    importSet.add("com.lealone.orm.json.JsonArray");
                    varInit = "        JsonArray ja = null;\r\n";
                    break;
                }
            }
            buffJsonMethod = new JsonServiceExecutorMethodGenerator().genCode(importSet,
                    "Object executeService(String methodName, String json)", varInit);
        }

        String executorPackageName = getExecutorPackageName();

        // 生成package和import代码
        String serviceImplementClassName = implementBy;
        if (implementBy != null) {
            if (implementBy.startsWith(executorPackageName)) {
                serviceImplementClassName = implementBy.substring(executorPackageName.length() + 1);
            } else {
                int lastDotPos = implementBy.lastIndexOf('.');
                if (lastDotPos > 0) {
                    serviceImplementClassName = implementBy.substring(lastDotPos + 1);
                    importSet.add(implementBy);
                }
            }
        }
        StringBuilder buff = new StringBuilder();
        buff.append("package ").append(executorPackageName).append(";\r\n");
        buff.append("\r\n");
        for (String i : importSet) {
            buff.append("import ").append(i).append(";\r\n");
        }
        buff.append("\r\n");

        // Service executor注释
        buff.append("/**\r\n");
        buff.append(" * Service executor for '").append(serviceName.toLowerCase()).append("'.\r\n");
        buff.append(" *\r\n");
        buff.append(" * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.\r\n");
        buff.append(" */\r\n");

        // 生成Service executor类的代码
        String className = getExecutorSimpleName();
        buff.append("public class ").append(className).append(" implements ServiceExecutor {\r\n");
        buff.append("\r\n");
        buff.append("    private final ").append(serviceImplementClassName).append(" si = new ")
                .append(serviceImplementClassName).append("();\r\n");

        // 生成默认构造函数
        // buff.append("\r\n");
        // buff.append(" public ").append(className).append("() {\r\n");
        // buff.append(" }\r\n");
        // buff.append("\r\n");

        buff.append(buffValueMethod);
        if (generateMapExecutorMethod)
            buff.append(buffMapMethod);
        if (generateJsonExecutorMethod)
            buff.append(buffJsonMethod);

        buff.append("}\r\n");
        if (writeFile)
            ModelCodeGenerator.writeFile(codePath, executorPackageName, className, buff);
        return buff;
    }

    private String getExecutorPackageName() {
        return packageName + ".executor";
    }

    private String getExecutorSimpleName() {
        return toClassName(serviceName) + "Executor";
    }

    private static String getTypeName(Column c, TreeSet<String> importSet) {
        return ModelCodeGenerator.getTypeName(c, importSet);
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
        case "BIGDECIMAL":
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
        case "OBJECT":
            return "ValueShort.get";
        case "BLOB":
            return "ValueShort.get";
        case "CLOB":
            return "ValueShort.get";
        case "ARRAY":
            return "ValueArray.get";
        case "RESULT_SET":
            return "ValueResultSet.get";
        }
        return "ValueString.get";
    }

    private static String m(String str, int i) {
        return str + "(ja.getValue(" + i + ").toString())";
    }

    // 根据具体类型调用合适的JsonArray方法
    private static String getJsonArrayMethodName(Column c, String type0, int i,
            TreeSet<String> importSet) {
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
        case "BIGDECIMAL":
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
        case "OBJECT":
            return "ja.getValue(" + i + ")";
        case "BLOB":
            return "new com.lealone.db.value.ReadonlyBlob(ja.getString(" + i + "))";
        case "CLOB":
            return "new com.lealone.db.value.ReadonlyClob(ja.getString(" + i + "))";
        case "ARRAY":
            return "new com.lealone.db.value.ReadonlyArray(ja.getString(" + i + "))";
        case "RESULT_SET":
            return "ja.getValue(" + i + ")";
        }
        if (c instanceof ListColumn)
            return "ja.getList(" + i + ")";
        if (c instanceof SetColumn)
            return "ja.getSet(" + i + ")";
        if (c instanceof MapColumn) {
            MapColumn mc = (MapColumn) c;
            return "ja.getMap(" + i + ", " + getTypeName(mc.key, importSet) + ".class)";
        }
        return type0 + ".decode(ja.getString(" + i + "))";
    }

    // 根据具体类型调用合适的Map方法
    private static String getMapMethodName(String type0) {
        String type = type0.toUpperCase();
        switch (type) {
        case "BOOLEAN":
            return "toBoolean";
        case "BYTE":
            return "toByte";
        case "SHORT":
            return "toShort";
        case "INTEGER":
            return "toInt";
        case "LONG":
            return "toLong";
        case "BIGDECIMAL":
            return "toBigDecimal";
        case "TIME":
            return "toTime";
        case "DATE":
            return "toDate";
        case "TIMESTAMP":
            return "toTimestamp";
        case "BYTE[]":
            return "toBytes";
        case "UUID":
            return "toUUID";
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "toString";
        case "DOUBLE":
            return "toDouble";
        case "FLOAT":
            return "toFloat";
        case "BLOB":
            return "toBlob";
        case "CLOB":
            return "toClob";
        case "ARRAY":
            return "toArray";
        case "NULL":
        case "UNKNOWN": // anything
        case "OBJECT":
            return "toObject";
        case "RESULT_SET":
            return "(java.sql.ResultSet) toObject";
        }
        return "toObject";
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
        case "OBJECT":
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
    }

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
            return "getBytes";
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "getString";
        case "DOUBLE":
            return "getDouble";
        case "FLOAT":
            return "getFloat";
        case "NULL":
            throw DbException.getInternalError();
        case "UNKNOWN": // anything
        case "OBJECT":
            return "getObject";
        case "BLOB":
            return "getBlob";
        case "CLOB":
            return "getClob";
        case "ARRAY":
            return "getArray";
        case "RESULT_SET":
            throw DbException.getInternalError();
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
        case "BIGDECIMAL":
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
            return "setBytes";
        case "STRING":
        case "STRING_IGNORECASE":
        case "STRING_FIXED":
            return "setString";
        case "DOUBLE":
            return "setDouble";
        case "FLOAT":
            return "setFloat";
        case "NULL":
            throw DbException.getInternalError();
        case "UNKNOWN": // anything
        case "OBJECT":
            return "setObject";
        case "BLOB":
            return "setBlob";
        case "CLOB":
            return "setClob";
        case "ARRAY":
            return "setArray";
        case "RESULT_SET":
            throw DbException.getInternalError();
        }
        return "setObject";
    }
}
