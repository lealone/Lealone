package org.lealone.test.service.generated.executer;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.sql.Date;
import org.lealone.db.service.ServiceExecuter;
import org.lealone.test.service.generated.User;
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
            User result1 = this.s.add(p_user1);
            return JsonObject.mapFrom(result1).encode();
        case "FIND":
            ja = new JsonArray(json);
            Long p_id2 = Long.valueOf(ja.getValue(0).toString());
            User result2 = this.s.find(p_id2);
            return JsonObject.mapFrom(result2).encode();
        case "FIND_BY_DATE":
            ja = new JsonArray(json);
            Date p_d3 = java.sql.Date.valueOf(ja.getValue(0).toString());
            User result3 = this.s.findByDate(p_d3);
            return JsonObject.mapFrom(result3).encode();
        case "UPDATE":
            ja = new JsonArray(json);
            User p_user4 = ja.getJsonObject(0).mapTo(User.class);
            Boolean result4 = this.s.update(p_user4);
            return result4.toString();
        case "DELETE":
            ja = new JsonArray(json);
            Long p_id5 = Long.valueOf(ja.getValue(0).toString());
            Boolean result5 = this.s.delete(p_id5);
            return result5.toString();
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }
}
