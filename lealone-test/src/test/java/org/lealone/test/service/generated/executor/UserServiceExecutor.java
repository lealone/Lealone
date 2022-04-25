package org.lealone.test.service.generated.executor;

import java.sql.Array;
import java.util.Map;
import org.lealone.db.service.ServiceExecutor;
import org.lealone.db.value.*;
import org.lealone.orm.json.JsonArray;
import org.lealone.test.orm.generated.User;
import org.lealone.test.service.impl.UserServiceImpl;

/**
 * Service executor for 'user_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class UserServiceExecutor implements ServiceExecutor {

    private final UserServiceImpl s = new UserServiceImpl();

    @Override
    public Value executeService(String methodName, Value[] methodArgs) {
        switch (methodName) {
        case "ADD":
            User p_user_1 = User.decode(methodArgs[0].getString());
            Long result1 = this.s.add(p_user_1);
            if (result1 == null)
                return ValueNull.INSTANCE;
            return ValueLong.get(result1);
        case "FIND":
            String p_name_2 = methodArgs[0].getString();
            User result2 = this.s.find(p_name_2);
            if (result2 == null)
                return ValueNull.INSTANCE;
            return ValueString.get(result2.encode());
        case "UPDATE":
            User p_user_3 = User.decode(methodArgs[0].getString());
            Integer result3 = this.s.update(p_user_3);
            if (result3 == null)
                return ValueNull.INSTANCE;
            return ValueInt.get(result3);
        case "GET_LIST":
            Array result4 = this.s.getList();
            if (result4 == null)
                return ValueNull.INSTANCE;
            return ValueArray.get(result4);
        case "DELETE":
            String p_name_5 = methodArgs[0].getString();
            Integer result5 = this.s.delete(p_name_5);
            if (result5 == null)
                return ValueNull.INSTANCE;
            return ValueInt.get(result5);
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }

    @Override
    public String executeService(String methodName, Map<String, Object> methodArgs) {
        switch (methodName) {
        case "ADD":
            User p_user_1 = User.decode(ServiceExecutor.toString("USER", methodArgs));
            Long result1 = this.s.add(p_user_1);
            if (result1 == null)
                return null;
            return result1.toString();
        case "FIND":
            String p_name_2 = ServiceExecutor.toString("NAME", methodArgs);
            User result2 = this.s.find(p_name_2);
            if (result2 == null)
                return null;
            return result2.encode();
        case "UPDATE":
            User p_user_3 = User.decode(ServiceExecutor.toString("USER", methodArgs));
            Integer result3 = this.s.update(p_user_3);
            if (result3 == null)
                return null;
            return result3.toString();
        case "GET_LIST":
            Array result4 = this.s.getList();
            if (result4 == null)
                return null;
            return result4.toString();
        case "DELETE":
            String p_name_5 = ServiceExecutor.toString("NAME", methodArgs);
            Integer result5 = this.s.delete(p_name_5);
            if (result5 == null)
                return null;
            return result5.toString();
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }

    @Override
    public String executeService(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "ADD":
            ja = new JsonArray(json);
            User p_user_1 = User.decode(ja.getString(0));
            Long result1 = this.s.add(p_user_1);
            if (result1 == null)
                return null;
            return result1.toString();
        case "FIND":
            ja = new JsonArray(json);
            String p_name_2 = ja.getString(0);
            User result2 = this.s.find(p_name_2);
            if (result2 == null)
                return null;
            return result2.encode();
        case "UPDATE":
            ja = new JsonArray(json);
            User p_user_3 = User.decode(ja.getString(0));
            Integer result3 = this.s.update(p_user_3);
            if (result3 == null)
                return null;
            return result3.toString();
        case "GET_LIST":
            Array result4 = this.s.getList();
            if (result4 == null)
                return null;
            return result4.toString();
        case "DELETE":
            ja = new JsonArray(json);
            String p_name_5 = ja.getString(0);
            Integer result5 = this.s.delete(p_name_5);
            if (result5 == null)
                return null;
            return result5.toString();
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }
}
