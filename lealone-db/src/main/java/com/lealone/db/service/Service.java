/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.service;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.lealone.agent.CodeAgent;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.CamelCaseHelper;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.IOUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.Database;
import com.lealone.db.DbObjectType;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.SysProperties;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.plugin.PluginManager;
import com.lealone.db.schema.Schema;
import com.lealone.db.schema.SchemaObject;
import com.lealone.db.schema.SchemaObjectBase;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.util.SourceCompiler;
import com.lealone.db.util.SourceCompiler.SCClassLoader;
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
    private String codePath;

    private volatile ServiceExecutor executor;
    private StringBuilder executorCode;

    private Map<String, Value> variables;
    private boolean workflow;

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

    public void setCodePath(String codePath) {
        this.codePath = codePath;
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

    public boolean isWorkflow() {
        return workflow;
    }

    public void setWorkflow(boolean workflow) {
        this.workflow = workflow;
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
                    Exception exception = null;
                    SCClassLoader classLoader = null;
                    try {
                        File classesDir = new File(getCodePath("classes"));
                        classLoader = SourceCompiler.getClassLoader(classesDir.toURI().toURL());
                        implementClass = classLoader.loadClass(getImplementBy());
                        return;
                    } catch (Exception e) {
                        exception = e;
                    }
                    Value llmProvider = variables.get("LLM_PROVIDER");
                    if (llmProvider != null) {
                        classLoader.addPackageName(packageName);
                        CodeAgent agent = getCodeAgent(llmProvider.getString());
                        if (isWorkflow()) {
                            genWorkflowCode(classLoader, agent);
                        } else {
                            genServiceCode(classLoader, agent);
                        }
                    } else {
                        throw DbException.convert(exception);
                    }
                    variables = null;
                }
            }
        }
    }

    private void genServiceCode(SCClassLoader classLoader, CodeAgent agent) {
        StringBuilder userPrompt = new StringBuilder(getCreateSQL());
        logger.info("Prompt:\n{}", getCreateSQL());
        genJavaCode(classLoader, agent, getTables(), userPrompt);
    }

    private void genWorkflowCode(SCClassLoader classLoader, CodeAgent agent) {
        StringBuilder userPrompt = new StringBuilder();
        for (SchemaObject so : schema.getAll(DbObjectType.SERVICE)) {
            if (!so.getName().equals(getName())) {
                if (!userPrompt.isEmpty())
                    userPrompt.append("、");
                userPrompt.append(((Service) so).getImplementBy());
            }
        }
        userPrompt.append("是服务接口实现类可以直接创建局部对象，优先使用服务接口，为").append(getName());
        userPrompt.append("生成一个工作流。").append('\n');
        for (SchemaObject so : schema.getAll(DbObjectType.SERVICE)) {
            if (!so.getName().equals(getName())) {
                userPrompt.append(so.getCreateSQL());
                userPrompt.append('\n');
            }
        }
        userPrompt.append(getCreateSQL());
        userPrompt.append('\n');
        logger.info("Prompt:\n{}", getCreateSQL());
        genJavaCode(classLoader, agent, schema.getAllTablesAndViews(), userPrompt);
    }

    private void genJavaCode(SCClassLoader classLoader, CodeAgent agent, List<Table> tables,
            StringBuilder userPrompt) {
        for (Table t : tables) {
            String className = toClassName(t.getName());
            String fullName = t.getPackageName() + "." + className;
            classLoader.addPackageName(t.getPackageName());
            userPrompt.append('\n');
            userPrompt.append("以下是" + className //
                    + "类，Model用findOne和findList,增加用insert：");
            userPrompt.append('\n');
            String code = t.getCode();
            if (code == null) {
                String src = t.getCodePath();
                if (src == null) {
                    src = getCodePath("src");
                }
                code = readSrcFile(src, t.getPackageName(), className);
                t.setCode(code);
            }
            userPrompt.append(code);
            try {
                classLoader.loadClass(fullName);
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
        // logger.info("Prompt:\n{}", userPrompt);
        String javaCode = agent.generateJavaCode(userPrompt.toString());
        logger.info("Java code:\n{}", javaCode);
        byte[] bytes = SourceCompiler.compile(classLoader, getImplementBy(), javaCode);
        implementClass = classLoader.getClass(getImplementBy(), bytes);
        writeFile(javaCode); // 编译成功再写
        writeClassFile(bytes);
    }

    private void setDefaultPackageName() {
        if (packageName == null)
            packageName = "service";
    }

    private String getSimpleName() {
        int pos = implementBy.indexOf('.');
        if (pos > 0)
            return implementBy.substring(pos + 1);
        else
            return implementBy;
    }

    private String getCodePath(String dir) {
        if (codePath == null)
            return new File(SysProperties.getBaseDir(), dir).getAbsolutePath();
        else
            return codePath;
    }

    private void writeFile(String javaCode) {
        setDefaultPackageName();
        writeFile(getCodePath("src"), packageName, getSimpleName(), new StringBuilder(javaCode));
    }

    private void writeClassFile(byte[] bytes) {
        setDefaultPackageName();
        writeClassFile(getCodePath("classes"), packageName, getSimpleName(), bytes);
    }

    // private byte[] readClassFile() {
    // setDefaultPackageName();
    // return readClassFile(getCodePath("classes"), packageName, getSimpleName());
    // }

    private List<Table> getTables() {
        ArrayList<Table> tables = new ArrayList<>();
        for (ServiceMethod m : serviceMethods) {
            for (Column c : m.getParameters()) {
                getTable(c, tables);
            }
            getTable(m.getReturnType(), tables);
        }
        return tables;
    }

    private void getTable(Column c, ArrayList<Table> tables) {
        Table t = c.getTable();
        if (t != null && t.getCode() != null)
            tables.add(t);
    }

    public static String toClassName(String n) {
        n = CamelCaseHelper.toCamelFromUnderscore(n);
        return Character.toUpperCase(n.charAt(0)) + n.substring(1);
    }

    private static String getPath(String codePath, String packageName) {
        String path = codePath;
        if (!path.endsWith(File.separator))
            path = path + File.separator;
        path = path.replace('/', File.separatorChar);
        path = path + packageName.replace('.', File.separatorChar) + File.separatorChar;
        return path;
    }

    // public static byte[] readClassFile(String codePath, String packageName, String className) {
    // String path = getPath(codePath, packageName);
    // try {
    // File classFile = new File(path, className + ".class");
    // if (!classFile.exists()) {
    // return null;
    // }
    // return IOUtils.toByteArray(new FileInputStream(classFile));
    // } catch (IOException e) {
    // throw DbException.convertIOException(e, "Failed to read file, path = " + path);
    // }
    // }

    public static void writeClassFile(String codePath, String packageName, String className,
            byte[] bytes) {
        String path = getPath(codePath, packageName);
        try {
            if (!new File(path).exists()) {
                new File(path).mkdirs();
            }
            BufferedOutputStream file = new BufferedOutputStream(
                    new FileOutputStream(path + className + ".class"));
            file.write(bytes);
            file.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Failed to write file, path = " + path);
        }
    }

    public static String readSrcFile(String codePath, String packageName, String className) {
        String path = getPath(codePath, packageName);
        try {
            File classFile = new File(path, className + ".java");
            if (!classFile.exists()) {
                return null;
            }
            return new String(IOUtils.toByteArray(new FileInputStream(classFile)), "UTF-8");
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Failed to read file, path = " + path);
        }
    }

    public static void writeFile(String codePath, String packageName, String className,
            StringBuilder code) {
        String path = getPath(codePath, packageName);
        try {
            if (!new File(path).exists()) {
                new File(path).mkdirs();
            }
            Charset utf8 = Charset.forName("UTF-8");
            BufferedOutputStream file = new BufferedOutputStream(
                    new FileOutputStream(path + className + ".java"));
            file.write(code.toString().getBytes(utf8));
            file.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Failed to write file, path = " + path);
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
