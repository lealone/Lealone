package org.lealone.test.generated.service.executer;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.lealone.db.service.ServiceExecuter;
import org.lealone.test.generated.model.User;
import org.lealone.test.service.impl.UserServiceImpl;

/**
 * Service executer for 'user_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class UserServiceExecuter implements ServiceExecuter {

    private final UserServiceImpl s = new UserServiceImpl();

    public UserServiceExecuter() {
    }

    @Override
    public String executeService(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "ADD":
            ja = new JsonArray(json);
            User p_user1 = ja.getJsonObject(0).mapTo(User.class);
            Long result1 = this.s.add(p_user1);
            if (result1 == null)
                return null;
            return result1.toString();
        case "FIND":
            ja = new JsonArray(json);
            String p_name2 = ja.getString(0);
            User result2 = this.s.find(p_name2);
            if (result2 == null)
                return null;
            return JsonObject.mapFrom(result2).encode();
        case "UPDATE":
            ja = new JsonArray(json);
            User p_user3 = ja.getJsonObject(0).mapTo(User.class);
            Integer result3 = this.s.update(p_user3);
            if (result3 == null)
                return null;
            return result3.toString();
        case "DELETE":
            ja = new JsonArray(json);
            String p_name4 = ja.getString(0);
            Integer result4 = this.s.delete(p_name4);
            if (result4 == null)
                return null;
            return result4.toString();
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }
}
