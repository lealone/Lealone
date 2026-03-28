/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.CamelCaseHelper;
import com.lealone.common.util.MapUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.Constants;
import com.lealone.db.service.Service;
import com.lealone.db.session.ServerSession;
import com.lealone.orm.Model;
import com.lealone.orm.json.Json;
import com.lealone.orm.json.JsonArray;

public class ServiceHandler {

    private static final Logger logger = LoggerFactory.getLogger(ServiceHandler.class);

    protected final String defaultDatabase;
    protected final String defaultSchema;
    protected final ServerSession session;

    public ServiceHandler(Map<String, String> config) {
        defaultDatabase = MapUtils.getString(config, "default_database", "lealone");
        defaultSchema = MapUtils.getString(config, "default_schema", "public");

        String url = config.get("jdbc_url");
        if (url == null)
            url = Constants.URL_PREFIX + Constants.URL_EMBED + defaultDatabase + ";password=;user=root";
        ConnectionInfo ci = new ConnectionInfo(url);
        session = (ServerSession) ci.createSession();
    }

    public ServerSession getSession() {
        return session;
    }

    public String executeService(String serviceName, String methodName, Map<String, Object> methodArgs) {
        return executeService(serviceName, methodName, methodArgs, false);
    }

    public String executeService(String serviceName, String methodName, Map<String, Object> methodArgs,
            boolean disableDynamicCompile) {
        if (methodArgs.containsKey("methodArgs")) {
            return executeService(serviceName, methodName, methodArgs.get("methodArgs").toString());
        }
        String[] serviceNameArray = StringUtils.arraySplit(serviceName, '.');
        if (serviceNameArray.length == 1 && defaultDatabase != null && defaultSchema != null)
            serviceName = defaultDatabase + "." + defaultSchema + "." + serviceName;
        else if (serviceNameArray.length == 2 && defaultDatabase != null)
            serviceName = defaultDatabase + "." + serviceName;

        Object result = null;
        try {
            logger.info("execute service: {}.{}", serviceName, methodName);
            if (serviceName.toUpperCase().contains("LEALONE_SYSTEM_SERVICE")) {
                result = SystemService.execute(serviceName, methodName, methodArgs);
            } else {
                result = Service.execute(session, serviceName, methodName, methodArgs,
                        disableDynamicCompile);
            }
        } catch (Exception e) {
            result = "failed to execute service: " + serviceName + "." + methodName + ", cause: "
                    + e.getMessage();
            logger.error(result, e);
            // 这种异常还是得抛给调用者
            if (e instanceof RuntimeException)
                throw e;
        }
        // 如果为null就返回"null"字符串
        if (result == null)
            result = "null";

        if (result instanceof List || result instanceof Set || result instanceof Map
                || result instanceof Model) {
            return Json.encode(result);
        }
        return result.toString();
    }

    public String executeService(String serviceName, String methodName, String methodArgs) {
        String command = "1;" + serviceName + "." + methodName + ";" + methodArgs;
        return executeService(command);
    }

    public String executeService(String command) {
        // 不能直接这样用: command.split(";");
        // 因为参数里可能包含分号
        int pos1 = command.indexOf(';');
        if (pos1 == -1) {
            return "invalid service: " + command;
        }
        String json;
        String oldServiceName;
        int type = Integer.parseInt(command.substring(0, pos1));
        int pos2 = command.indexOf(';', pos1 + 1);
        if (pos2 == -1) {
            json = "[]"; // 没有参数
            oldServiceName = command.substring(pos1 + 1);
        } else {
            json = command.substring(pos2 + 1);
            oldServiceName = command.substring(pos1 + 1, pos2);
        }

        String serviceName = CamelCaseHelper.toUnderscoreFromCamel(oldServiceName);
        String[] serviceNameArray = StringUtils.arraySplit(serviceName, '.');
        if (serviceNameArray.length == 2 && defaultDatabase != null && defaultSchema != null)
            serviceName = defaultDatabase + "." + defaultSchema + "." + serviceName;
        else if (serviceNameArray.length == 3 && defaultDatabase != null)
            serviceName = defaultDatabase + "." + serviceName;
        JsonArray ja = new JsonArray();
        Object result = null;
        switch (type) {
        case 1:
            try {
                logger.info("execute service: " + serviceName);
                if (serviceName.toUpperCase().contains("LEALONE_SYSTEM_SERVICE")) {
                    result = SystemService.execute(serviceName, json);
                } else {
                    result = Service.execute(getSession(), serviceName, json);
                }
                ja.add(2);
            } catch (Exception e) {
                ja.add(3);
                result = "failed to execute service: " + serviceName + ", cause: " + e.getMessage();
                logger.error(result, e);
            }
            break;
        default:
            ja.add(3);
            result = "unknown request type: " + type + ", serviceName: " + serviceName;
            logger.error(result);
        }
        // 如果为null就返回"null"字符串
        if (result == null)
            result = "null";
        ja.add(oldServiceName); // 前端传来的方法名不一定是下划线风格的，所以用最初的
        ja.add(result.toString());
        return ja.toString();
    }
}
