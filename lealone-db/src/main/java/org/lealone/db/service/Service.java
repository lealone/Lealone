/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import java.util.List;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.SchemaObjectBase;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;

public class Service extends SchemaObjectBase {

    private String language;
    private String packageName;
    private String implementBy;
    private final String sql;
    private final String serviceExecutorClassName;
    private final List<ServiceMethod> serviceMethods;

    private ServiceExecutor executor;

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

    public List<ServiceMethod> getServiceMethods() {
        return serviceMethods;
    }

    @Override
    public String getCreateSQL() {
        return sql;
    }

    public void setExecutor(ServiceExecutor executor) {
        this.executor = executor;
    }

    // 延迟创建executor的实例，因为执行create service语句时，依赖的服务实现类还不存在
    public ServiceExecutor getExecutor() {
        if (executor == null) {
            synchronized (this) {
                try {
                    if (executor == null)
                        executor = (ServiceExecutor) Class.forName(serviceExecutorClassName).getDeclaredConstructor()
                                .newInstance();
                } catch (Exception e) {
                    throw new RuntimeException("newInstance exception: " + serviceExecutorClassName, e);
                }
            }
        }
        return executor;
    }

    public static Service getService(ServerSession session, Database db, String schemaName, String serviceName) {
        // 调用服务前数据库可能没有初始化
        if (!db.isInitialized())
            db.init();
        Schema schema = db.findSchema(session, schemaName);
        if (schema == null) {
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        Service service = schema.findService(session, serviceName);
        if (service != null) {
            return service;
        } else {
            throw new RuntimeException("service " + serviceName + " not found");
        }
    }

    // 通过jdbc调用
    public static Value execute(ServerSession session, String serviceName, String methodName, Value[] methodArgs) {
        Service service = getService(session, session.getDatabase(), session.getCurrentSchemaName(), serviceName);
        return service.getExecutor().executeService(methodName, methodArgs);
    }

    // 通过http调用
    public static String execute(String serviceName, String methodName, Map<String, Object> methodArgs) {
        serviceName = serviceName.toUpperCase();
        methodName = methodName.toUpperCase();
        String[] a = StringUtils.arraySplit(serviceName, '.');
        if (a.length == 3) {
            Database db = LealoneDatabase.getInstance().getDatabase(a[0]);
            Service service = getService(null, db, a[1], a[2]);
            return service.getExecutor().executeService(methodName, methodArgs);
        } else {
            throw new RuntimeException("service " + serviceName + " not found");
        }
    }

    // 通过sockjs调用
    public static String execute(String serviceName, String json) {
        serviceName = serviceName.toUpperCase();
        String[] a = StringUtils.arraySplit(serviceName, '.');
        if (a.length == 4) {
            Database db = LealoneDatabase.getInstance().getDatabase(a[0]);
            Service service = getService(null, db, a[1], a[2]);
            return service.getExecutor().executeService(a[3], json);
        } else {
            throw new RuntimeException("service " + serviceName + " not found");
        }
    }
}
