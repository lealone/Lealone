package org.lealone.test.vertx.generated;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.lealone.test.vertx.generated.User;

/**
 * Service interface for 'user_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface UserService {

    static UserService create(String url) {
        return new Proxy(url);
    }

    User add(User user);

    User find(Long id);

    User findByDate(Date d);

    Boolean update(User user);

    Boolean delete(Long id);

    static class Proxy implements UserService {

        private final String url;
        private static final String sqlWithReturnValue = "{? = call executeServiceNoReturnValue(?,?)}";

        private Proxy(String url) {
            this.url = url;
        }

        @Override
        public User add(User user) {
            JsonArray ja = new JsonArray();
            ja.add(JsonObject.mapFrom(user));
            String result = executeWithReturnValue("USER_SERVICE.ADD", ja.encode());
            if (result != null) {
                JsonObject jo = new JsonObject(result);
                return jo.mapTo(User.class);
            }
            return null;
        }

        @Override
        public User find(Long id) {
            JsonArray ja = new JsonArray();
            ja.add(id);
            String result = executeWithReturnValue("USER_SERVICE.FIND", ja.encode());
            if (result != null) {
                JsonObject jo = new JsonObject(result);
                return jo.mapTo(User.class);
            }
            return null;
        }

        @Override
        public User findByDate(Date d) {
            JsonArray ja = new JsonArray();
            ja.add(d);
            String result = executeWithReturnValue("USER_SERVICE.FIND_BY_DATE", ja.encode());
            if (result != null) {
                JsonObject jo = new JsonObject(result);
                return jo.mapTo(User.class);
            }
            return null;
        }

        @Override
        public Boolean update(User user) {
            JsonArray ja = new JsonArray();
            ja.add(JsonObject.mapFrom(user));
            String result = executeWithReturnValue("USER_SERVICE.UPDATE", ja.encode());
            if (result != null) {
                return Boolean.valueOf(result);
            }
            return null;
        }

        @Override
        public Boolean delete(Long id) {
            JsonArray ja = new JsonArray();
            ja.add(id);
            String result = executeWithReturnValue("USER_SERVICE.DELETE", ja.encode());
            if (result != null) {
                return Boolean.valueOf(result);
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
                e.printStackTrace();
            }

            return null;
        }
    }
}
