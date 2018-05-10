package org.lealone.test.service.generated;

import io.vertx.core.json.JsonArray;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Service interface for 'hello_world_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface HelloWorldService {

    static HelloWorldService create(String url) {
        return new Proxy(url);
    }

    void sayHello();

    String sayGoodbyeTo(String name);

    static class Proxy implements HelloWorldService {

        private final String url;
        private static final String sqlNoReturnValue = "{call EXECUTE_SERVICE_NO_RETURN_VALUE(?,?)}";
        private static final String sqlWithReturnValue = "{? = call EXECUTE_SERVICE_WITH_RETURN_VALUE(?,?)}";

        private Proxy(String url) {
            this.url = url;
        }

        @Override
        public void sayHello() {
            JsonArray ja = new JsonArray();
            executeNoReturnValue("HELLO_WORLD_SERVICE.SAY_HELLO", ja.encode());
        }

        @Override
        public String sayGoodbyeTo(String name) {
            JsonArray ja = new JsonArray();
            ja.add(name);
            String result = executeWithReturnValue("HELLO_WORLD_SERVICE.SAY_GOODBYE_TO", ja.encode());
            if (result != null) {
                return result;
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
                throw new RuntimeException("Failted to execute service: " + serviceName);
            }

            return null;
        }

        private void executeNoReturnValue(String serviceName, String json) {
            try (Connection conn = DriverManager.getConnection(url);
                    CallableStatement stmt = conn.prepareCall(sqlNoReturnValue)) {
                stmt.setString(1, serviceName);
                stmt.setString(2, json);
                stmt.execute();
            } catch (SQLException e) {
                throw new RuntimeException("Failted to execute service: " + serviceName);
            }
        }
    }
}
