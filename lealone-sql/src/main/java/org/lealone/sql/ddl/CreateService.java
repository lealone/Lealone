/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.PluginManager;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.service.JavaServiceExecutorFactory;
import org.lealone.db.service.Service;
import org.lealone.db.service.ServiceExecutor;
import org.lealone.db.service.ServiceExecutorFactory;
import org.lealone.db.service.ServiceMethod;
import org.lealone.db.service.ServiceSetting;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Column.ListColumn;
import org.lealone.db.table.Column.MapColumn;
import org.lealone.db.table.Column.SetColumn;
import org.lealone.db.table.CreateTableData;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueUuid;
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
    private String language = "java";
    private String packageName;
    private String implementBy;
    private boolean genCode;
    private String codePath;
    private CaseInsensitiveMap<String> serviceParameters;

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

    @Override
    public boolean isIfDDL() {
        return ifNotExists;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setLanguage(String language) {
        this.language = language;
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

    public void setServiceParameters(CaseInsensitiveMap<String> serviceParameters) {
        this.serviceParameters = serviceParameters;
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
        // 替换变量，重建SQL
        if (sql.indexOf('@') >= 0) {
            sql = getCreateSQL();
        }
        int methodSize = this.serviceMethods.size();
        ArrayList<ServiceMethod> serviceMethods = new ArrayList<>(methodSize);
        for (int methodIndex = 0; methodIndex < methodSize; methodIndex++) {
            CreateTableData data = this.serviceMethods.get(methodIndex).data;
            ServiceMethod m = new ServiceMethod();
            m.setMethodName(data.tableName);
            int size = data.columns.size() - 1;
            ArrayList<Column> parameters = new ArrayList<>(size);
            m.setParameters(parameters);
            m.setReturnType(data.columns.get(size)); // 最后一列是返回类型
            for (int i = 0; i < size; i++) {
                parameters.add(data.columns.get(i));
            }
            serviceMethods.add(m);
        }

        boolean isJava = "java".equalsIgnoreCase(language);
        if (isJava && implementBy != null && packageName == null) {
            int pos = implementBy.lastIndexOf('.');
            if (pos > 0)
                packageName = implementBy.substring(0, pos);
            else
                packageName = "";
        }

        // 非启动阶段，如果java服务实现类不存在时自动生成一个
        if (!session.getDatabase().isStarting() && isJava && implementBy != null) {
            try {
                Class.forName(implementBy);
            } catch (Exception e) {
                if (codePath == null)
                    codePath = "./src/main/java";
                genServiceImplementClassCode();
            }
        }

        Service service = new Service(schema, id, serviceName, sql, getExecutorFullName(),
                serviceMethods);
        service.setLanguage(language);
        service.setImplementBy(implementBy);
        service.setPackageName(packageName);
        service.setComment(comment);
        schema.add(session, service, lock);

        ServiceExecutorFactory factory = PluginManager.getPlugin(ServiceExecutorFactory.class, language);
        if (factory == null)
            factory = new JavaServiceExecutorFactory();

        // 数据库在启动阶段执行CREATE SERVICE语句时不用再生成代码
        if (!session.getDatabase().isStarting()) {
            if (factory.supportsGenCode()) { // 支持其他语言插件生成自己的代码
                factory.genCode(service);
            } else {
                if (genCode) {
                    genServiceInterfaceCode();
                    genServiceExecutorCode(true);
                }
            }
        }
        // 最后才创建执行器，此时implementBy肯定存在了
        // 如果没有提前生成ServiceExecutor才去使用JavaServiceExecutor
        if (!genCode) {
            if (isJava) {
                service.setExecutorCode(genServiceExecutorCode(false));
            } else {
                ServiceExecutor executor = factory.createServiceExecutor(service);
                service.setExecutor(executor);
            }
        }
        return 0;
    }

    private String getCreateSQL() {
        StatementBuilder buff = new StatementBuilder("CREATE SERVICE ");
        buff.append(session.getDatabase().quoteIdentifier(serviceName));
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append(" (\n");
        int methodSize = serviceMethods.size();
        for (int methodIndex = 0; methodIndex < methodSize; methodIndex++) {
            CreateTableData data = serviceMethods.get(methodIndex).data;
            buff.append("    ").append(session.getDatabase().quoteIdentifier(data.tableName))
                    .append("(");
            // 最后一列是返回类型所以要减一
            for (int i = 0, size = data.columns.size() - 1; i < size; i++) {
                Column column = data.columns.get(i);
                if (i != 0) {
                    buff.append(", ");
                }
                buff.append(column.getCreateSQL());
            }
            // 返回类型不需要生成列名
            buff.append(") ").append(data.columns.get(data.columns.size() - 1).getCreateSQL(true));
            if (methodIndex != methodSize - 1) {
                buff.append(",");
            }
            buff.append("\n");
        }
        buff.append(")\n");
        if (language != null) {
            buff.append("LANGUAGE '").append(language).append("'\n");
        }
        if (packageName != null) {
            buff.append("PACKAGE '").append(packageName).append("'\n");
        }
        if (implementBy != null) {
            buff.append("IMPLEMENT BY '").append(implementBy).append("'\n");
        }
        if (genCode && codePath != null) {
            buff.append("GENERATE CODE '").append(codePath).append("'\n");
        }
        if (codePath != null) {
            buff.append("CODE PATH '").append(codePath).append("'\n");
        }
        if (serviceParameters != null && !serviceParameters.isEmpty()) {
            buff.append(" PARAMETERS");
            Database.appendMap(buff, serviceParameters);
        }
        return buff.toString();
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

    private void genServiceInterfaceCode() {
        // PreparedStatement字段声明
        StringBuilder psBuff = new StringBuilder();
        // PreparedStatement字段初始化
        StringBuilder psInitBuff = new StringBuilder();

        TreeSet<String> importSet = new TreeSet<>();
        importSet.add("org.lealone.client.ClientServiceProxy");
        importSet.add("java.sql.*");

        // 生成方法签名和方法体的代码
        int methodSize = serviceMethods.size();
        ArrayList<StringBuilder> methodSignatureList = new ArrayList<>(methodSize);
        ArrayList<StringBuilder> proxyMethodBodyList = new ArrayList<>(methodSize);

        for (int methodIndex = 0; methodIndex < methodSize; methodIndex++) {
            StringBuilder methodSignatureBuff = new StringBuilder();
            StringBuilder proxyMethodBodyBuff = new StringBuilder();
            String psVarName = "ps" + (methodIndex + 1);
            CreateTableData data = serviceMethods.get(methodIndex).data;

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

        writeFile(codePath, packageName, serviceInterfaceName, buff);
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

    private void genServiceImplementClassCode() {
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
            CreateTableData data = serviceMethods.get(methodIndex).data;

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

        writeFile(codePath, implementPackageName, implementClassName, buff);
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
                CreateTableData data = m.data;

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

    private StringBuilder genServiceExecutorCode(boolean writeFile) {
        TreeSet<String> importSet = new TreeSet<>();
        importSet.add(ServiceExecutor.class.getName());

        // 生成public Value executeService(String methodName, Value[] methodArgs)方法
        importSet.add("org.lealone.db.value.*");
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
                if (m.data.columns.size() - 1 > 0) {
                    importSet.add("org.lealone.plugins.orm.json.JsonArray");
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
            writeFile(codePath, executorPackageName, className, buff);
        return buff;
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

    public static void writeFile(String codePath, String packageName, String className,
            StringBuilder... buffArray) {
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
            BufferedOutputStream file = new BufferedOutputStream(
                    new FileOutputStream(path + className + ".java"));
            for (StringBuilder buff : buffArray) {
                file.write(buff.toString().getBytes(utf8));
            }
            file.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Failed to genJavaCode, path = " + path);
        }
    }

    static String getTypeName(Column c, TreeSet<String> importSet) {
        String cType;
        if (c instanceof ListColumn) {
            importSet.add("java.util.List");
            ListColumn lc = (ListColumn) c;
            cType = "List<" + getTypeName(lc.element, importSet) + ">";
            return cType;
        } else if (c instanceof SetColumn) {
            importSet.add("java.util.Set");
            SetColumn sc = (SetColumn) c;
            cType = "Set<" + getTypeName(sc.element, importSet) + ">";
            return cType;
        } else if (c instanceof MapColumn) {
            importSet.add("java.util.Map");
            MapColumn mc = (MapColumn) c;
            cType = "Map<" + getTypeName(mc.key, importSet) + ", " + getTypeName(mc.value, importSet)
                    + ">";
            return cType;
        }
        if (c.getTable() != null) {
            cType = c.getTable().getName();
            cType = toClassName(cType);
            String packageName = c.getTable().getPackageName();
            if (packageName != null)
                cType = packageName + "." + cType;
        } else {
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
            return "new org.lealone.db.value.ReadonlyBlob(ja.getString(" + i + "))";
        case "CLOB":
            return "new org.lealone.db.value.ReadonlyClob(ja.getString(" + i + "))";
        case "ARRAY":
            return "new org.lealone.db.value.ReadonlyArray(ja.getString(" + i + "))";
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
