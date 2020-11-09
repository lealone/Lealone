package org.lealone.test.service.generated.executor;

import java.util.Map;
import org.lealone.db.service.ServiceExecutor;
import org.lealone.db.value.*;
import org.lealone.orm.json.JsonArray;
import org.lealone.orm.json.JsonObject;
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
            User p_user_1 =  new JsonObject(methodArgs[0].getString()).mapTo(User.class);
            Long result1 = this.s.add(p_user_1);
            if (result1 == null)
                return ValueNull.INSTANCE;
            return ValueLong.get(result1);
        case "FIND":
            String p_name_2 = methodArgs[0].getString();
            User result2 = this.s.find(p_name_2);
            if (result2 == null)
                return ValueNull.INSTANCE;
            return ValueString.get(JsonObject.mapFrom(result2).encode());
        case "UPDATE":
            User p_user_3 =  new JsonObject(methodArgs[0].getString()).mapTo(User.class);
            Integer result3 = this.s.update(p_user_3);
            if (result3 == null)
                return ValueNull.INSTANCE;
            return ValueInt.get(result3);
        case "DELETE":
            String p_name_4 = methodArgs[0].getString();
            Integer result4 = this.s.delete(p_name_4);
            if (result4 == null)
                return ValueNull.INSTANCE;
            return ValueInt.get(result4);
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }

    @Override
    public String executeService(String methodName, Map<String, String> methodArgs) {
        switch (methodName) {
        case "ADD":
            User p_user_1 =  new JsonObject(methodArgs.get("USER")).mapTo(User.class);
            Long result1 = this.s.add(p_user_1);
            if (result1 == null)
                return null;
            return result1.toString();
        case "FIND":
            String p_name_2 = methodArgs.get("NAME");
            User result2 = this.s.find(p_name_2);
            if (result2 == null)
                return null;
            return JsonObject.mapFrom(result2).encode();
        case "UPDATE":
            User p_user_3 =  new JsonObject(methodArgs.get("USER")).mapTo(User.class);
            Integer result3 = this.s.update(p_user_3);
            if (result3 == null)
                return null;
            return result3.toString();
        case "DELETE":
            String p_name_4 = methodArgs.get("NAME");
            Integer result4 = this.s.delete(p_name_4);
            if (result4 == null)
                return null;
            return result4.toString();
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
            User p_user_1 = ja.getJsonObject(0).mapTo(User.class);
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
            return JsonObject.mapFrom(result2).encode();
        case "UPDATE":
            ja = new JsonArray(json);
            User p_user_3 = ja.getJsonObject(0).mapTo(User.class);
            Integer result3 = this.s.update(p_user_3);
            if (result3 == null)
                return null;
            return result3.toString();
        case "DELETE":
            ja = new JsonArray(json);
            String p_name_4 = ja.getString(0);
            Integer result4 = this.s.delete(p_name_4);
            if (result4 == null)
                return null;
            return result4.toString();
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }
}
