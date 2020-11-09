package org.lealone.test.service.generated;

import java.sql.*;
import java.sql.Date;
import org.lealone.client.ClientServiceProxy;

/**
 * Service interface for 'hello_world_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface HelloWorldService {

    static HelloWorldService create(String url) {
        if (new org.lealone.db.ConnectionInfo(url).isEmbedded())
            return new org.lealone.test.service.impl.HelloWorldServiceImpl();
        else
            return new ServiceProxy(url);
    }

    void sayHello();

    Date getDate();

    Integer getInt();

    Integer getTwo(String name, Integer age);

    String sayGoodbyeTo(String name);

    static class ServiceProxy implements HelloWorldService {

        private final PreparedStatement ps1;
        private final PreparedStatement ps2;
        private final PreparedStatement ps3;
        private final PreparedStatement ps4;
        private final PreparedStatement ps5;

        private ServiceProxy(String url) {
            ps1 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE SAY_HELLO()");
            ps2 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE GET_DATE()");
            ps3 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE GET_INT()");
            ps4 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE GET_TWO(?, ?)");
            ps5 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE SAY_GOODBYE_TO(?)");
        }

        @Override
        public void sayHello() {
            try {
                ps1.executeUpdate();
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("HELLO_WORLD_SERVICE.SAY_HELLO", e);
            }
        }

        @Override
        public Date getDate() {
            try {
                ResultSet rs = ps2.executeQuery();
                rs.next();
                Date ret =  rs.getDate(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_DATE", e);
            }
        }

        @Override
        public Integer getInt() {
            try {
                ResultSet rs = ps3.executeQuery();
                rs.next();
                Integer ret =  rs.getInt(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_INT", e);
            }
        }

        @Override
        public Integer getTwo(String name, Integer age) {
            try {
                ps4.setString(1, name);
                ps4.setInt(2, age);
                ResultSet rs = ps4.executeQuery();
                rs.next();
                Integer ret =  rs.getInt(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_TWO", e);
            }
        }

        @Override
        public String sayGoodbyeTo(String name) {
            try {
                ps5.setString(1, name);
                ResultSet rs = ps5.executeQuery();
                rs.next();
                String ret =  rs.getString(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("HELLO_WORLD_SERVICE.SAY_GOODBYE_TO", e);
            }
        }
    }
}
