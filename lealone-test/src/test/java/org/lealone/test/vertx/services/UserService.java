package org.lealone.test.vertx.services;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public interface UserService {

    static UserService create(String url) {
        return new UserServiceProxy(url);
    }

    User add(User user);

    User find(long id);

    boolean update(User user);

    boolean delete(long id);

    static class UserServiceProxy implements UserService {

        private static final String sqlWithReturnValue = "{? = call executeServiceWithReturnValue(?,?)}";
        private final String url;

        private UserServiceProxy(String url) {
            this.url = url;
        }

        @Override
        public User add(User user) {
            try (Connection conn = DriverManager.getConnection(url);
                    CallableStatement stmt = conn.prepareCall(sqlWithReturnValue)) {
                JsonArray ja = new JsonArray();
                ja.add(JsonObject.mapFrom(user));
                stmt.setString(2, "user_service.add");
                stmt.setString(3, ja.encode());
                stmt.registerOutParameter(1, java.sql.Types.VARCHAR);
                if (stmt.execute()) {
                    JsonObject jo = new JsonObject(stmt.getString(1));
                    user = jo.mapTo(User.class);
                    return user;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public User find(long id) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean update(User user) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean delete(long id) {
            // TODO Auto-generated method stub
            return false;
        }
    }
}

// class UserServiceProxy implements UserService {
//
// private static final String sqlWithReturnValue = "{? = call executeServiceWithReturnValue(?,?)}";
// private final String url;
//
// UserServiceProxy(String url) {
// this.url = url;
// }
//
// @Override
// public User add(User user) {
// try (Connection conn = DriverManager.getConnection(url);
// CallableStatement stmt = conn.prepareCall(sqlWithReturnValue)) {
// stmt.setString(2, "user_service.add");
// stmt.setString(3, JsonObject.mapFrom(user).toString());
// stmt.registerOutParameter(1, java.sql.Types.VARCHAR);
// if (stmt.execute()) {
// JsonObject jo = new JsonObject(stmt.getString(1));
// user = jo.mapTo(User.class);
// return user;
// }
// } catch (SQLException e) {
// e.printStackTrace();
// }
// return null;
// }
//
// @Override
// public User find(long id) {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public boolean update(User user) {
// // TODO Auto-generated method stub
// return false;
// }
//
// @Override
// public boolean delete(long id) {
// // TODO Auto-generated method stub
// return false;
// }
// }
