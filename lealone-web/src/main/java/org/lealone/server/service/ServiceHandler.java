/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.service;

import java.util.Map;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.StringUtils;
import org.lealone.db.service.Service;

public class ServiceHandler {

    private static final Logger logger = LoggerFactory.getLogger(ServiceHandler.class);

    protected final String defaultDatabase;
    protected final String defaultSchema;

    public ServiceHandler(Map<String, String> config) {
        defaultDatabase = config.get("default_database");
        defaultSchema = config.get("default_schema");
    }

    public String executeService(String serviceName, String methodName, Map<String, Object> methodArgs) {
        String[] serviceNameArray = StringUtils.arraySplit(serviceName, '.');
        if (serviceNameArray.length == 1 && defaultDatabase != null && defaultSchema != null)
            serviceName = defaultDatabase + "." + defaultSchema + "." + serviceName;
        else if (serviceNameArray.length == 2 && defaultDatabase != null)
            serviceName = defaultDatabase + "." + serviceName;

        String result = null;
        try {
            logger.info("execute service: {}.{}", serviceName, methodName);
            if (serviceName.toUpperCase().contains("LEALONE_SYSTEM_SERVICE")) {
                result = SystemService.execute(serviceName, methodName, methodArgs);
            } else {
                result = Service.execute(serviceName, methodName, methodArgs);
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
        return result;
    }
}
