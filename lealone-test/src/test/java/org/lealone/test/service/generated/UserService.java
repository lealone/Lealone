package org.lealone.test.service.generated;

import java.sql.*;
import org.lealone.client.ClientServiceProxy;
import org.lealone.orm.json.JsonObject;
import org.lealone.test.orm.generated.User;

/**
 * Service interface for 'user_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface UserService {

    static UserService create(String url) {
        if (new org.lealone.db.ConnectionInfo(url).isEmbedded())
            return new org.lealone.test.service.impl.UserServiceImpl();
        else
            return new ServiceProxy(url);
    }

    Long add(User user);

    User find(String name);

    Integer update(User user);

    Integer delete(String name);

    static class ServiceProxy implements UserService {

        private final PreparedStatement ps1;
        private final PreparedStatement ps2;
        private final PreparedStatement ps3;
        private final PreparedStatement ps4;

        private ServiceProxy(String url) {
            ps1 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE ADD(?)");
            ps2 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE FIND(?)");
            ps3 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE UPDATE(?)");
            ps4 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE DELETE(?)");
        }

        @Override
        public Long add(User user) {
            try {
                ps1.setString(1, JsonObject.mapFrom(user).encode());
                ResultSet rs = ps1.executeQuery();
                rs.next();
                Long ret =  rs.getLong(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("USER_SERVICE.ADD", e);
            }
        }

        @Override
        public User find(String name) {
            try {
                ps2.setString(1, name);
                ResultSet rs = ps2.executeQuery();
                rs.next();
                JsonObject jo = new JsonObject(rs.getString(1));
                rs.close();
                return jo.mapTo(User.class);
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("USER_SERVICE.FIND", e);
            }
        }

        @Override
        public Integer update(User user) {
            try {
                ps3.setString(1, JsonObject.mapFrom(user).encode());
                ResultSet rs = ps3.executeQuery();
                rs.next();
                Integer ret =  rs.getInt(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("USER_SERVICE.UPDATE", e);
            }
        }

        @Override
        public Integer delete(String name) {
            try {
                ps4.setString(1, name);
                ResultSet rs = ps4.executeQuery();
                rs.next();
                Integer ret =  rs.getInt(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("USER_SERVICE.DELETE", e);
            }
        }
    }
}
