package org.lealone.test.vertx.impl;

import org.lealone.test.vertx.services.User;
import org.lealone.vertx.ServiceExecuter;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class UserServiceExecuter implements ServiceExecuter {

    private final UserServiceImpl s = new UserServiceImpl();

    public UserServiceExecuter() {
    }

    @Override
    public String executeServiceWithReturnValue(String methodName, String json) {
        switch (methodName) {
        case "add": {
            JsonArray ja = new JsonArray(json);
            JsonObject jo = ja.getJsonObject(0);
            User user = jo.mapTo(User.class);
            user = s.add(user);
            return JsonObject.mapFrom(user).toString();
        }
        default: {
            System.out.println("no method: " + methodName);
        }
        }

        return "";
    }

    @Override
    public void executeServiceNoReturnValue(String serviceName, String json) {
        // TODO Auto-generated method stub

    }

}
