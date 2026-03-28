/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.service;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.lealone.agent.CodeAgent;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.Database;
import com.lealone.db.DbObjectType;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.plugin.PluginManager;
import com.lealone.db.schema.Schema;
import com.lealone.db.schema.SchemaObjectBase;
import com.lealone.db.session.ServerSession;
import com.lealone.db.util.SourceCompiler;
import com.lealone.db.value.Value;

public class Service extends SchemaObjectBase {

    private static final Logger logger = LoggerFactory.getLogger(Service.class);

    private String language;
    private String packageName;
    private String implementBy;
    private volatile Class<?> implementClass;
    private final String sql;
    private final String serviceExecutorClassName;
    private final List<ServiceMethod> serviceMethods;

    private volatile ServiceExecutor executor;
    private StringBuilder executorCode;

    private Map<String, Value> variables;

    public Service(Schema schema, int id, String name, String sql, String serviceExecutorClassName,
            List<ServiceMethod> serviceMethods) {
        super(schema, id, name);
        this.sql = sql;
        this.serviceExecutorClassName = serviceExecutorClassName;
        this.serviceMethods = serviceMethods;
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.SERVICE;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getImplementBy() {
        return implementBy;
    }

    public void setImplementBy(String implementBy) {
        this.implementBy = implementBy;
    }

    public Class<?> getImplementClass() {
        return implementClass;
    }

    public List<ServiceMethod> getServiceMethods() {
        return serviceMethods;
    }

    @Override
    public String getCreateSQL() {
        return sql;
    }

    public Map<String, Value> getVariables() {
        return variables;
    }

    public void setVariables(Map<String, Value> variables) {
        this.variables = variables;
    }

    public void setExecutorCode(StringBuilder executorCode) {
        this.executorCode = executorCode;
    }

    public void setExecutor(ServiceExecutor executor) {
        this.executor = executor;
    }

    // 延迟创建executor的实例，因为执行create service语句时，依赖的服务实现类还不存在
    public ServiceExecutor getExecutor() {
        return getExecutor(false);
    }

    public ServiceExecutor getExecutor(boolean disableDynamicCompile) {
        if (executor == null) {
            synchronized (this) {
                if (executor == null) {
                    // 跟spring boot集成时不支持动态编译
                    if (disableDynamicCompile) {
                        executor = new JavaServiceExecutor(this);
                        return executor;
                    }
                    if (executorCode != null) {
                        String code = executorCode.toString();
                        executorCode = null;
                        executor = SourceCompiler.compileAsInstance(serviceExecutorClassName, code);
                    } else {
                        executor = Utils.newInstance(serviceExecutorClassName);
                    }
                }
            }
        }
        return executor;
    }

    private CodeAgent getCodeAgent(String llmProvider) {
        CodeAgent agent = PluginManager.getPlugin(CodeAgent.class, llmProvider);
        if (agent == null)
            throw DbException.get(ErrorCode.PLUGIN_NOT_FOUND_1, llmProvider);
        CaseInsensitiveMap<String> parameters = new CaseInsensitiveMap<>(variables.size());
        for (Entry<String, Value> e : variables.entrySet()) {
            parameters.put(e.getKey(), e.getValue().getString());
        }
        agent.init(parameters);
        return agent;
    }

    private void init() {
        if (implementClass == null) {
            synchronized (this) {
                if (implementClass == null) {
                    Value llmProvider = variables.get("LLM_PROVIDER");
                    if (llmProvider != null) {
                        CodeAgent agent = getCodeAgent(llmProvider.getString());
                        String userPrompt = getCreateSQL();
                        logger.info("Service sql:\n{}", userPrompt);
                        String javaCode = agent.generateJavaCode(userPrompt);
                        logger.info("Java code:\n{}", javaCode);
                        implementClass = SourceCompiler.compileAsClass(getImplementBy(), javaCode);
                    } else {
                        try {
                            implementClass = Class.forName(getImplementBy());
                        } catch (Exception e) {
                            DbException.convert(e);
                        }
                    }
                    variables = null;
                }
            }
        }
    }

    public static Service getService(ServerSession session, Database db, String schemaName,
            String serviceName) {
        // 调用服务前数据库可能没有初始化
        if (!db.isInitialized())
            db.init();
        Schema schema = db.findSchema(session, schemaName);
        if (schema == null) {
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        Service service = schema.getService(session, serviceName);
        service.init();
        return service;
    }

    private static void checkRight(ServerSession session, Service service) {
        session.getUser().checkRight(service, Right.EXECUTE);
    }

    // 通过jdbc调用
    public static Value execute(ServerSession session, String serviceName, String methodName,
            Value[] methodArgs) {
        Service service = getService(session, session.getDatabase(), session.getCurrentSchemaName(),
                serviceName);
        checkRight(session, service);
        return service.getExecutor().executeService(methodName, methodArgs);
    }

    // 通过http调用
    public static Object execute(ServerSession session, String serviceName, String methodName,
            Map<String, Object> methodArgs) {
        return execute(session, serviceName, methodName, methodArgs, false);
    }

    public static Object execute(ServerSession session, String serviceName, String methodName,
            Map<String, Object> methodArgs, boolean disableDynamicCompile) {
        String[] a = StringUtils.arraySplit(serviceName, '.');
        if (a.length == 3) {
            Database db = LealoneDatabase.getInstance().getDatabase(a[0]);
            if (db.getSettings().databaseToUpper) {
                serviceName = serviceName.toUpperCase();
                methodName = methodName.toUpperCase();
            }
            Service service = getService(session, db, a[1], a[2]);
            checkRight(session, service);
            return service.getExecutor(disableDynamicCompile).executeService(methodName, methodArgs);
        } else {
            throw new RuntimeException("service " + serviceName + " not found");
        }
    }

    // 通过json调用
    public static Object execute(ServerSession session, String serviceName, String json) {
        String[] a = StringUtils.arraySplit(serviceName, '.');
        if (a.length == 4) {
            Database db = LealoneDatabase.getInstance().getDatabase(a[0]);
            String methodName = a[3];
            if (db.getSettings().databaseToUpper) {
                methodName = methodName.toUpperCase();
            }
            Service service = getService(session, db, a[1], a[2]);
            checkRight(session, service);
            return service.getExecutor().executeService(methodName, json);
        } else {
            throw new RuntimeException("service " + serviceName + " not found");
        }
    }
}
