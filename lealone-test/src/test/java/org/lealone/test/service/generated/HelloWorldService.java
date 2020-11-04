package org.lealone.test.service.generated;

import java.sql.*;
import java.sql.Date;
import org.lealone.client.ClientServiceProxy;
import org.lealone.orm.json.JsonArray;

/**
 * Service interface for 'hello_world_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface HelloWorldService {

    static HelloWorldService create(String url) {
        if (new org.lealone.db.ConnectionInfo(url).isEmbedded())
            return new org.lealone.test.service.impl.HelloWorldServiceImpl();
        else;
            return new JdbcProxy(url);
    }

    void sayHello();

    Date getDate();

    Integer getInt();

    Integer getTwo(String name, Integer age);

    String sayGoodbyeTo(String name);

    static class Proxy implements HelloWorldService {

        private final String url;

        private Proxy(String url) {
            this.url = url;
        }

        @Override
        public void sayHello() {
            JsonArray ja = new JsonArray();
            ClientServiceProxy.executeNoReturnValue(url, "HELLO_WORLD_SERVICE.SAY_HELLO", ja.encode());
        }

        @Override
        public Date getDate() {
            JsonArray ja = new JsonArray();
            String result = ClientServiceProxy.executeWithReturnValue(url, "HELLO_WORLD_SERVICE.GET_DATE", ja.encode());
            if (result != null) {
                return java.sql.Date.valueOf(result);
            }
            return null;
        }

        @Override
        public Integer getInt() {
            JsonArray ja = new JsonArray();
            String result = ClientServiceProxy.executeWithReturnValue(url, "HELLO_WORLD_SERVICE.GET_INT", ja.encode());
            if (result != null) {
                return Integer.valueOf(result);
            }
            return null;
        }

        @Override
        public Integer getTwo(String name, Integer age) {
            JsonArray ja = new JsonArray();
            ja.add(name);
            ja.add(age);
            String result = ClientServiceProxy.executeWithReturnValue(url, "HELLO_WORLD_SERVICE.GET_TWO", ja.encode());
            if (result != null) {
                return Integer.valueOf(result);
            }
            return null;
        }

        @Override
        public String sayGoodbyeTo(String name) {
            JsonArray ja = new JsonArray();
            ja.add(name);
            String result = ClientServiceProxy.executeWithReturnValue(url, "HELLO_WORLD_SERVICE.SAY_GOODBYE_TO", ja.encode());
            if (result != null) {
                return result;
            }
            return null;
        }
    }

    static class JdbcProxy implements HelloWorldService {

        private final PreparedStatement ps1;
        private final PreparedStatement ps2;
        private final PreparedStatement ps3;
        private final PreparedStatement ps4;
        private final PreparedStatement ps5;

        private JdbcProxy(String url) {
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
                Date ret = rs.getDate(1);
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
                Integer ret = rs.getInt(1);
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
                Integer ret = rs.getInt(1);
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
                String ret = rs.getString(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("HELLO_WORLD_SERVICE.SAY_GOODBYE_TO", e);
            }
        }
    }
}
