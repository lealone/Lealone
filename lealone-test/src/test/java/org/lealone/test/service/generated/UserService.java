package org.lealone.test.service.generated;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.sql.Date;
import org.lealone.client.ClientServiceProxy;
import org.lealone.test.service.generated.User;

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

        private Proxy(String url) {
            this.url = url;
        }

        @Override
        public User add(User user) {
            JsonArray ja = new JsonArray();
            ja.add(JsonObject.mapFrom(user));
            String result = ClientServiceProxy.executeWithReturnValue(url, "USER_SERVICE.ADD", ja.encode());
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
            String result = ClientServiceProxy.executeWithReturnValue(url, "USER_SERVICE.FIND", ja.encode());
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
            String result = ClientServiceProxy.executeWithReturnValue(url, "USER_SERVICE.FIND_BY_DATE", ja.encode());
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
            String result = ClientServiceProxy.executeWithReturnValue(url, "USER_SERVICE.UPDATE", ja.encode());
            if (result != null) {
                return Boolean.valueOf(result);
            }
            return null;
        }

        @Override
        public Boolean delete(Long id) {
            JsonArray ja = new JsonArray();
            ja.add(id);
            String result = ClientServiceProxy.executeWithReturnValue(url, "USER_SERVICE.DELETE", ja.encode());
            if (result != null) {
                return Boolean.valueOf(result);
            }
            return null;
        }
    }
}
