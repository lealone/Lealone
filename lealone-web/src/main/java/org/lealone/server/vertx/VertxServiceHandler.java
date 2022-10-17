/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.vertx;

import java.util.Map;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.common.util.StringUtils;
import org.lealone.db.service.Service;
import org.lealone.server.service.ServiceHandler;
import org.lealone.server.service.SystemService;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;

public class VertxServiceHandler extends ServiceHandler implements Handler<SockJSSocket> {

    private static final Logger logger = LoggerFactory.getLogger(VertxServiceHandler.class);

    public VertxServiceHandler(Map<String, String> config) {
        super(config);
    }

    @Override
    public void handle(SockJSSocket sockJSSocket) {
        sockJSSocket.exceptionHandler(t -> {
            logger.error("sockJSSocket exception", t);
        });

        sockJSSocket.handler(buffer -> {
            String command = buffer.getString(0, buffer.length());
            Buffer ret = Buffer.buffer(executeService(command));
            sockJSSocket.end(ret);
        });
    }

    public String executeService(String serviceName, String methodName, String methodArgs) {
        String command = "1;" + serviceName + "." + methodName + ";" + methodArgs;
        return executeService(command);
    }

    private String executeService(String command) {
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
        String result = null;
        switch (type) {
        case 1:
            try {
                logger.info("execute service: " + serviceName);
                if (serviceName.toUpperCase().contains("LEALONE_SYSTEM_SERVICE")) {
                    result = SystemService.execute(serviceName, json);
                } else {
                    result = Service.execute(serviceName, json);
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
        ja.add(oldServiceName); // 前端传来的方法名不一定是下划线风格的，所以用最初的
        ja.add(result);
        return ja.toString();
    }
}
