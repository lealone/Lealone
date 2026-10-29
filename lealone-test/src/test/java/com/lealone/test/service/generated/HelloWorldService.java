package com.lealone.test.service.generated;

import com.lealone.client.ClientServiceProxy;
import com.lealone.client.jdbc.JdbcPreparedStatement;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import java.sql.*;
import java.sql.Date;

/**
 * Service interface for 'hello_world_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface HelloWorldService {

    default void sayHello() {
        sayHelloAsync().get();
    }

    default Date getDate() {
        return getDateAsync().get();
    }

    default Integer getInt() {
        return getIntAsync().get();
    }

    default Integer getTwo(String name, Integer age) {
        return getTwoAsync(name, age).get();
    }

    default String sayGoodbyeTo(String name) {
        return sayGoodbyeToAsync(name).get();
    }

    Future<Void> sayHelloAsync();

    Future<Date> getDateAsync();

    Future<Integer> getIntAsync();

    Future<Integer> getTwoAsync(String name, Integer age);

    Future<String> sayGoodbyeToAsync(String name);

    static HelloWorldService _create() {
        return _create(null);
    }

    static HelloWorldService _create(String url) {
        if (url == null)
            url = ClientServiceProxy.getUrl();

        if (ClientServiceProxy.isEmbedded(url))
            return new com.lealone.test.service.impl.HelloWorldServiceImpl();
        else
            return new ServiceProxy(url);
    }

    static class ServiceProxy implements HelloWorldService {

        private final JdbcPreparedStatement ps1;
        private final JdbcPreparedStatement ps2;
        private final JdbcPreparedStatement ps3;
        private final JdbcPreparedStatement ps4;
        private final JdbcPreparedStatement ps5;

        private ServiceProxy(String url) {
            ps1 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE SAY_HELLO()");
            ps2 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE GET_DATE()");
            ps3 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE GET_INT()");
            ps4 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE GET_TWO(?, ?)");
            ps5 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE HELLO_WORLD_SERVICE SAY_GOODBYE_TO(?)");
        }

        @Override
        public Future<Void> sayHelloAsync() {
            AsyncCallback<Void> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps1.executeUpdateAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("HELLO_WORLD_SERVICE.SAY_HELLO", ar.getCause(), ac);
                        return;
                    }
                    ac.setAsyncResult((Void) null);
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("HELLO_WORLD_SERVICE.SAY_HELLO", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Date> getDateAsync() {
            AsyncCallback<Date> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps2.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_DATE", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        Date ret = rs.getDate(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_DATE", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_DATE", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Integer> getIntAsync() {
            AsyncCallback<Integer> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps3.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_INT", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        Integer ret = rs.getInt(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_INT", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_INT", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Integer> getTwoAsync(String name, Integer age) {
            AsyncCallback<Integer> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps4.setString(1, name);
                ps4.setInt(2, age);
                ps4.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_TWO", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        Integer ret = rs.getInt(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_TWO", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("HELLO_WORLD_SERVICE.GET_TWO", e, ac);
            }
            return ac;
        }

        @Override
        public Future<String> sayGoodbyeToAsync(String name) {
            AsyncCallback<String> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps5.setString(1, name);
                ps5.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("HELLO_WORLD_SERVICE.SAY_GOODBYE_TO", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        String ret = rs.getString(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("HELLO_WORLD_SERVICE.SAY_GOODBYE_TO", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("HELLO_WORLD_SERVICE.SAY_GOODBYE_TO", e, ac);
            }
            return ac;
        }
    }
}
