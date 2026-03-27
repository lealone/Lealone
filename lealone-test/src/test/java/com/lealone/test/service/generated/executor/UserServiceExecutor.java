package com.lealone.test.service.generated.executor;

import com.lealone.db.service.ServiceExecutor;
import com.lealone.db.value.*;
import com.lealone.orm.json.JsonArray;
import com.lealone.test.orm.generated.User;
import com.lealone.test.service.impl.UserServiceImpl;
import java.sql.Array;
import java.util.Map;

/**
 * Service executor for 'user_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class UserServiceExecutor implements ServiceExecutor {

    private final UserServiceImpl si = new UserServiceImpl();

    @Override
    public Value executeService(String methodName, Value[] methodArgs) {
        switch (methodName) {
        case "ADD":
            User p_user_1 = User.decode(methodArgs[0].getString());
            Long result1 = si.add(p_user_1);
            if (result1 == null)
                return ValueNull.INSTANCE;
            return ValueLong.get(result1);
        case "FIND":
            String p_name_2 = methodArgs[0].getString();
            User result2 = si.find(p_name_2);
            if (result2 == null)
                return ValueNull.INSTANCE;
            return ValueString.get(result2.encode());
        case "UPDATE":
            User p_user_3 = User.decode(methodArgs[0].getString());
            Integer result3 = si.update(p_user_3);
            if (result3 == null)
                return ValueNull.INSTANCE;
            return ValueInt.get(result3);
        case "GET_LIST":
            Array result4 = si.getList();
            if (result4 == null)
                return ValueNull.INSTANCE;
            return ValueArray.get(result4);
        case "DELETE":
            String p_name_5 = methodArgs[0].getString();
            Integer result5 = si.delete(p_name_5);
            if (result5 == null)
                return ValueNull.INSTANCE;
            return ValueInt.get(result5);
        default:
            throw noMethodException(methodName);
        }
    }

    @Override
    public Object executeService(String methodName, Map<String, Object> methodArgs) {
        switch (methodName) {
        case "ADD":
            User p_user_1 = User.decode(toString("USER", methodArgs));
            return si.add(p_user_1);
        case "FIND":
            String p_name_2 = toString("NAME", methodArgs);
            return si.find(p_name_2);
        case "UPDATE":
            User p_user_3 = User.decode(toString("USER", methodArgs));
            return si.update(p_user_3);
        case "GET_LIST":
            return si.getList();
        case "DELETE":
            String p_name_5 = toString("NAME", methodArgs);
            return si.delete(p_name_5);
        default:
            throw noMethodException(methodName);
        }
    }

    @Override
    public Object executeService(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "ADD":
            ja = new JsonArray(json);
            User p_user_1 = User.decode(ja.getString(0));
            return si.add(p_user_1);
        case "FIND":
            ja = new JsonArray(json);
            String p_name_2 = ja.getString(0);
            return si.find(p_name_2);
        case "UPDATE":
            ja = new JsonArray(json);
            User p_user_3 = User.decode(ja.getString(0));
            return si.update(p_user_3);
        case "GET_LIST":
            return si.getList();
        case "DELETE":
            ja = new JsonArray(json);
            String p_name_5 = ja.getString(0);
            return si.delete(p_name_5);
        default:
            throw noMethodException(methodName);
        }
    }
}
