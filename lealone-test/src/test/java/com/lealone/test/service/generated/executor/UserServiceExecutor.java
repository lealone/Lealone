package com.lealone.test.service.generated.executor;

import com.lealone.db.async.Future;
import com.lealone.db.service.ServiceExecutor;
import com.lealone.db.value.*;
import com.lealone.orm.json.JsonArray;
import com.lealone.test.orm.generated.User;
import com.lealone.test.service.impl.UserServiceImpl;
import java.util.Map;

/**
 * Service executor for 'user_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class UserServiceExecutor implements ServiceExecutor {

    private final UserServiceImpl si = new UserServiceImpl();

    @Override
    public Future<?> executeServiceAsync(String methodName, Value[] methodArgs) {
        switch (methodName) {
        case "ADD":
            User p_user_1 = User.decode(methodArgs[0]);
            return si.addAsync(p_user_1);
        case "FIND":
            String p_name_2 = methodArgs[0].getString();
            return si.findAsync(p_name_2);
        case "UPDATE":
            User p_user_3 = User.decode(methodArgs[0]);
            return si.updateAsync(p_user_3);
        case "GET_LIST":
            return si.getListAsync();
        case "DELETE":
            String p_name_5 = methodArgs[0].getString();
            return si.deleteAsync(p_name_5);
        default:
            return Future.failedFuture(noMethodException(methodName));
        }
    }

    @Override
    public Future<?> executeServiceAsync(String methodName, Map<String, Object> methodArgs) {
        switch (methodName) {
        case "ADD":
            User p_user_1 = User.decode(toString("USER", methodArgs));
            return si.addAsync(p_user_1);
        case "FIND":
            String p_name_2 = toString("NAME", methodArgs);
            return si.findAsync(p_name_2);
        case "UPDATE":
            User p_user_3 = User.decode(toString("USER", methodArgs));
            return si.updateAsync(p_user_3);
        case "GET_LIST":
            return si.getListAsync();
        case "DELETE":
            String p_name_5 = toString("NAME", methodArgs);
            return si.deleteAsync(p_name_5);
        default:
            return Future.failedFuture(noMethodException(methodName));
        }
    }

    @Override
    public Future<?> executeServiceAsync(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "ADD":
            ja = new JsonArray(json);
            User p_user_1 = User.decode(ja.getString(0));
            return si.addAsync(p_user_1);
        case "FIND":
            ja = new JsonArray(json);
            String p_name_2 = ja.getString(0);
            return si.findAsync(p_name_2);
        case "UPDATE":
            ja = new JsonArray(json);
            User p_user_3 = User.decode(ja.getString(0));
            return si.updateAsync(p_user_3);
        case "GET_LIST":
            return si.getListAsync();
        case "DELETE":
            ja = new JsonArray(json);
            String p_name_5 = ja.getString(0);
            return si.deleteAsync(p_name_5);
        default:
            return Future.failedFuture(noMethodException(methodName));
        }
    }
}
