package org.lealone.test.fullstack.generated;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.lealone.test.fullstack.generated.User;

/**
 * Service interface for 'user_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface UserService {

    static UserService create(String url) {
        return new Proxy(url);
    }

    Long add(User user);

    User find(String name);

    Integer update(User user);

    Integer delete(String name);

    static class Proxy implements UserService {

        private final String url;
        private static final String sqlWithReturnValue = "{? = call EXECUTE_SERVICE_WITH_RETURN_VALUE(?,?)}";

        private Proxy(String url) {
            this.url = url;
        }

        @Override
        public Long add(User user) {
            JsonArray ja = new JsonArray();
            ja.add(JsonObject.mapFrom(user));
            String result = executeWithReturnValue("USER_SERVICE.ADD", ja.encode());
            if (result != null) {
                return Long.valueOf(result);
            }
            return null;
        }

        @Override
        public User find(String name) {
            JsonArray ja = new JsonArray();
            ja.add(name);
            String result = executeWithReturnValue("USER_SERVICE.FIND", ja.encode());
            if (result != null) {
                JsonObject jo = new JsonObject(result);
                return jo.mapTo(User.class);
            }
            return null;
        }

        @Override
        public Integer update(User user) {
            JsonArray ja = new JsonArray();
            ja.add(JsonObject.mapFrom(user));
            String result = executeWithReturnValue("USER_SERVICE.UPDATE", ja.encode());
            if (result != null) {
                return Integer.valueOf(result);
            }
            return null;
        }

        @Override
        public Integer delete(String name) {
            JsonArray ja = new JsonArray();
            ja.add(name);
            String result = executeWithReturnValue("USER_SERVICE.DELETE", ja.encode());
            if (result != null) {
                return Integer.valueOf(result);
            }
            return null;
        }

        private String executeWithReturnValue(String serviceName, String json) {
            try (Connection conn = DriverManager.getConnection(url);
                    CallableStatement stmt = conn.prepareCall(sqlWithReturnValue)) {
                stmt.setString(2, serviceName);
                stmt.setString(3, json);
                stmt.registerOutParameter(1, java.sql.Types.VARCHAR);
                if (stmt.execute()) {
                    return stmt.getString(1);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failted to execute service: " + serviceName, e);
            }

            return null;
        }
    }
}
