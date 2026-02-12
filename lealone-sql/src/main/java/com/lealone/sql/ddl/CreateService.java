/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.ddl;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.CamelCaseHelper;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.StatementBuilder;
import com.lealone.common.util.StringUtils;
import com.lealone.db.Database;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.plugin.PluginManager;
import com.lealone.db.schema.Schema;
import com.lealone.db.service.JavaServiceExecutorFactory;
import com.lealone.db.service.Service;
import com.lealone.db.service.ServiceCodeGenerator;
import com.lealone.db.service.ServiceExecutor;
import com.lealone.db.service.ServiceExecutorFactory;
import com.lealone.db.service.ServiceMethod;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.CreateTableData;
import com.lealone.sql.SQLStatement;

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
    private String codeGenerator;

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

    public void setCodeGenerator(String codeGenerator) {
        this.codeGenerator = codeGenerator;
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

        Service service = new Service(schema, id, serviceName, sql, getExecutorFullName(),
                serviceMethods);
        service.setLanguage(language);
        service.setImplementBy(implementBy);
        service.setPackageName(packageName);
        service.setComment(comment);
        schema.add(session, service, lock);

        // 非启动阶段，如果java服务实现类不存在时自动生成一个
        if (!session.getDatabase().isStarting() && isJava && implementBy != null) {
            try {
                Class.forName(implementBy);
            } catch (Exception e) {
                if (codePath == null)
                    codePath = "./src/main/java";
                ServiceCodeGenerator generator = getServiceCodeGenerator(service);
                generator.genServiceImplementClassCode();
            }
        }

        ServiceExecutorFactory factory = PluginManager.getPlugin(ServiceExecutorFactory.class, language);
        if (factory == null)
            factory = new JavaServiceExecutorFactory();

        // 数据库在启动阶段执行CREATE SERVICE语句时不用再生成代码
        if (!session.getDatabase().isStarting()) {
            if (factory.supportsGenCode()) { // 支持其他语言插件生成自己的代码
                factory.genCode(service);
            } else {
                if (genCode) {
                    ServiceCodeGenerator generator = getServiceCodeGenerator(service);
                    generator.genServiceInterfaceCode();
                    generator.genServiceExecutorCode(true);
                }
            }
        }
        // 最后才创建执行器，此时implementBy肯定存在了
        // 如果没有提前生成ServiceExecutor才去使用JavaServiceExecutor
        if (!genCode) {
            if (isJava && methodSize > 0 && findServiceCodeGenerator(service) != null) {
                ServiceCodeGenerator generator = getServiceCodeGenerator(service);
                service.setExecutorCode(generator.genServiceExecutorCode(false));
            } else {
                ServiceExecutor executor = factory.createServiceExecutor(service);
                service.setExecutor(executor);
            }
        }
        return 0;
    }

    private ServiceCodeGenerator findServiceCodeGenerator(Service service) {
        if (codeGenerator == null)
            codeGenerator = "default_service_code_generator";
        return PluginManager.getPlugin(ServiceCodeGenerator.class, codeGenerator);
    }

    private ServiceCodeGenerator getServiceCodeGenerator(Service service) {
        ServiceCodeGenerator generator = findServiceCodeGenerator(service);
        if (generator == null)
            throw DbException.get(ErrorCode.PLUGIN_NOT_FOUND_1, codeGenerator);
        generator.init(service, serviceMethods, serviceParameters, codePath, genCode);
        return generator;
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
        if (genCode && codeGenerator != null) {
            buff.append("GENERATOR NAME '").append(codeGenerator).append("'\n");
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

    private String getExecutorPackageName() {
        return packageName + ".executor";
    }

    private String getExecutorFullName() {
        return getExecutorPackageName() + "." + getExecutorSimpleName();
    }

    private String getExecutorSimpleName() {
        return toClassName(serviceName) + "Executor";
    }

    // 外部插件也用到了
    public static String toClassName(String n) {
        n = CamelCaseHelper.toCamelFromUnderscore(n);
        return Character.toUpperCase(n.charAt(0)) + n.substring(1);
    }
}
