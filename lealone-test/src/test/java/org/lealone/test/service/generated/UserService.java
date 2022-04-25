package org.lealone.test.service.generated;

import java.sql.*;
import java.sql.Array;
import org.lealone.client.ClientServiceProxy;
import org.lealone.test.orm.generated.User;

/**
 * Service interface for 'user_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface UserService {

    Long add(User user);

    User find(String name);

    Integer update(User user);

    Array getList();

    Integer delete(String name);

    static UserService create() {
        return create(null);
    }

    static UserService create(String url) {
        if (url == null)
            url = ClientServiceProxy.getUrl();

        if (ClientServiceProxy.isEmbedded(url))
            return new org.lealone.test.service.impl.UserServiceImpl();
        else
            return new ServiceProxy(url);
    }

    static class ServiceProxy implements UserService {

        private final PreparedStatement ps1;
        private final PreparedStatement ps2;
        private final PreparedStatement ps3;
        private final PreparedStatement ps4;
        private final PreparedStatement ps5;

        private ServiceProxy(String url) {
            ps1 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE ADD(?)");
            ps2 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE FIND(?)");
            ps3 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE UPDATE(?)");
            ps4 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE GET_LIST()");
            ps5 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE DELETE(?)");
        }

        @Override
        public Long add(User user) {
            try {
                ps1.setString(1, user.encode());
                ResultSet rs = ps1.executeQuery();
                rs.next();
                Long ret = rs.getLong(1);
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
                String ret = rs.getString(1);
                rs.close();
                return User.decode(ret);
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("USER_SERVICE.FIND", e);
            }
        }

        @Override
        public Integer update(User user) {
            try {
                ps3.setString(1, user.encode());
                ResultSet rs = ps3.executeQuery();
                rs.next();
                Integer ret = rs.getInt(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("USER_SERVICE.UPDATE", e);
            }
        }

        @Override
        public Array getList() {
            try {
                ResultSet rs = ps4.executeQuery();
                rs.next();
                Array ret = rs.getArray(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("USER_SERVICE.GET_LIST", e);
            }
        }

        @Override
        public Integer delete(String name) {
            try {
                ps5.setString(1, name);
                ResultSet rs = ps5.executeQuery();
                rs.next();
                Integer ret = rs.getInt(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("USER_SERVICE.DELETE", e);
            }
        }
    }
}
