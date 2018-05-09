package org.lealone.test.vertx.services;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public interface HelloWorldService {

    static HelloWorldService create(String url) {
        return new Proxy(url);
    }

    void sayHello();

    static class Proxy implements HelloWorldService {
        private final String url;
        private static final String sqlNoReturnValue = "{call executeServiceNoReturnValue(?,?)}";

        private Proxy(String url) {
            this.url = url;
        }

        @Override
        public void sayHello() {
            try (Connection conn = DriverManager.getConnection(url);
                    CallableStatement stmt = conn.prepareCall(sqlNoReturnValue)) {
                stmt.setString(1, "hello_world_service.sayHello");
                stmt.setString(2, "");
                stmt.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
