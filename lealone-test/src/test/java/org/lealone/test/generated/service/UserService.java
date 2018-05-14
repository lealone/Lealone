package org.lealone.test.generated.service;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.lealone.client.ClientServiceProxy;
import org.lealone.test.generated.model.User;

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

        private Proxy(String url) {
            this.url = url;
        }

        @Override
        public Long add(User user) {
            JsonArray ja = new JsonArray();
            ja.add(JsonObject.mapFrom(user));
            String result = ClientServiceProxy.executeWithReturnValue(url, "USER_SERVICE.ADD", ja.encode());
            if (result != null) {
                return Long.valueOf(result);
            }
            return null;
        }

        @Override
        public User find(String name) {
            JsonArray ja = new JsonArray();
            ja.add(name);
            String result = ClientServiceProxy.executeWithReturnValue(url, "USER_SERVICE.FIND", ja.encode());
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
            String result = ClientServiceProxy.executeWithReturnValue(url, "USER_SERVICE.UPDATE", ja.encode());
            if (result != null) {
                return Integer.valueOf(result);
            }
            return null;
        }

        @Override
        public Integer delete(String name) {
            JsonArray ja = new JsonArray();
            ja.add(name);
            String result = ClientServiceProxy.executeWithReturnValue(url, "USER_SERVICE.DELETE", ja.encode());
            if (result != null) {
                return Integer.valueOf(result);
            }
            return null;
        }
    }
}
