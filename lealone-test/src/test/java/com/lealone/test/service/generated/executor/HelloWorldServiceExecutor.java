package com.lealone.test.service.generated.executor;

import com.lealone.db.service.ServiceExecutor;
import com.lealone.db.value.*;
import com.lealone.orm.json.JsonArray;
import com.lealone.test.service.impl.HelloWorldServiceImpl;
import java.sql.Date;
import java.util.Map;

/**
 * Service executor for 'hello_world_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class HelloWorldServiceExecutor implements ServiceExecutor {

    private final HelloWorldServiceImpl si = new HelloWorldServiceImpl();

    @Override
    public Value executeService(String methodName, Value[] methodArgs) {
        switch (methodName) {
        case "SAY_HELLO":
            si.sayHello();
            return ValueNull.INSTANCE;
        case "GET_DATE":
            Date result2 = si.getDate();
            if (result2 == null)
                return ValueNull.INSTANCE;
            return ValueDate.get(result2);
        case "GET_INT":
            Integer result3 = si.getInt();
            if (result3 == null)
                return ValueNull.INSTANCE;
            return ValueInt.get(result3);
        case "GET_TWO":
            String p_name_4 = methodArgs[0].getString();
            Integer p_age_4 = methodArgs[1].getInt();
            Integer result4 = si.getTwo(p_name_4, p_age_4);
            if (result4 == null)
                return ValueNull.INSTANCE;
            return ValueInt.get(result4);
        case "SAY_GOODBYE_TO":
            String p_name_5 = methodArgs[0].getString();
            String result5 = si.sayGoodbyeTo(p_name_5);
            if (result5 == null)
                return ValueNull.INSTANCE;
            return ValueString.get(result5);
        default:
            throw noMethodException(methodName);
        }
    }

    @Override
    public Object executeService(String methodName, Map<String, Object> methodArgs) {
        switch (methodName) {
        case "SAY_HELLO":
            si.sayHello();
            return NO_RETURN_VALUE;
        case "GET_DATE":
            return si.getDate();
        case "GET_INT":
            return si.getInt();
        case "GET_TWO":
            String p_name_4 = toString("NAME", methodArgs);
            Integer p_age_4 = toInt("AGE", methodArgs);
            return si.getTwo(p_name_4, p_age_4);
        case "SAY_GOODBYE_TO":
            String p_name_5 = toString("NAME", methodArgs);
            return si.sayGoodbyeTo(p_name_5);
        default:
            throw noMethodException(methodName);
        }
    }

    @Override
    public Object executeService(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "SAY_HELLO":
            si.sayHello();
            return NO_RETURN_VALUE;
        case "GET_DATE":
            return si.getDate();
        case "GET_INT":
            return si.getInt();
        case "GET_TWO":
            ja = new JsonArray(json);
            String p_name_4 = ja.getString(0);
            Integer p_age_4 = Integer.valueOf(ja.getValue(1).toString());
            return si.getTwo(p_name_4, p_age_4);
        case "SAY_GOODBYE_TO":
            ja = new JsonArray(json);
            String p_name_5 = ja.getString(0);
            return si.sayGoodbyeTo(p_name_5);
        default:
            throw noMethodException(methodName);
        }
    }
}
