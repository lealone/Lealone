package com.lealone.test.service.generated.executor;

import com.lealone.db.async.Future;
import com.lealone.db.service.ServiceExecutor;
import com.lealone.db.value.*;
import com.lealone.orm.json.JsonArray;
import com.lealone.test.service.impl.HelloWorldServiceImpl;
import java.util.Map;

/**
 * Service executor for 'hello_world_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class HelloWorldServiceExecutor implements ServiceExecutor {

    private final HelloWorldServiceImpl si = new HelloWorldServiceImpl();

    @Override
    public Future<?> executeServiceAsync(String methodName, Value[] methodArgs) {
        switch (methodName) {
        case "SAY_HELLO":
            return si.sayHelloAsync();
        case "GET_DATE":
            return si.getDateAsync();
        case "GET_INT":
            return si.getIntAsync();
        case "GET_TWO":
            String p_name_4 = methodArgs[0].getString();
            Integer p_age_4 = methodArgs[1].getInt();
            return si.getTwoAsync(p_name_4, p_age_4);
        case "SAY_GOODBYE_TO":
            String p_name_5 = methodArgs[0].getString();
            return si.sayGoodbyeToAsync(p_name_5);
        default:
            return Future.failedFuture(noMethodException(methodName));
        }
    }

    @Override
    public Future<?> executeServiceAsync(String methodName, Map<String, Object> methodArgs) {
        switch (methodName) {
        case "SAY_HELLO":
            return si.sayHelloAsync();
        case "GET_DATE":
            return si.getDateAsync();
        case "GET_INT":
            return si.getIntAsync();
        case "GET_TWO":
            String p_name_4 = toString("NAME", methodArgs);
            Integer p_age_4 = toInt("AGE", methodArgs);
            return si.getTwoAsync(p_name_4, p_age_4);
        case "SAY_GOODBYE_TO":
            String p_name_5 = toString("NAME", methodArgs);
            return si.sayGoodbyeToAsync(p_name_5);
        default:
            return Future.failedFuture(noMethodException(methodName));
        }
    }

    @Override
    public Future<?> executeServiceAsync(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "SAY_HELLO":
            return si.sayHelloAsync();
        case "GET_DATE":
            return si.getDateAsync();
        case "GET_INT":
            return si.getIntAsync();
        case "GET_TWO":
            ja = new JsonArray(json);
            String p_name_4 = ja.getString(0);
            Integer p_age_4 = Integer.valueOf(ja.getValue(1).toString());
            return si.getTwoAsync(p_name_4, p_age_4);
        case "SAY_GOODBYE_TO":
            ja = new JsonArray(json);
            String p_name_5 = ja.getString(0);
            return si.sayGoodbyeToAsync(p_name_5);
        default:
            return Future.failedFuture(noMethodException(methodName));
        }
    }
}
