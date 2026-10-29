package com.lealone.test.service.generated;

import com.lealone.client.ClientServiceProxy;
import com.lealone.client.jdbc.JdbcPreparedStatement;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import com.lealone.test.orm.generated.User;
import java.sql.*;
import java.sql.Array;

/**
 * Service interface for 'user_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface UserService {

    default Long add(User user) {
        return addAsync(user).get();
    }

    default User find(String name) {
        return findAsync(name).get();
    }

    default Integer update(User user) {
        return updateAsync(user).get();
    }

    default Array getList() {
        return getListAsync().get();
    }

    default Integer delete(String name) {
        return deleteAsync(name).get();
    }

    Future<Long> addAsync(User user);

    Future<User> findAsync(String name);

    Future<Integer> updateAsync(User user);

    Future<Array> getListAsync();

    Future<Integer> deleteAsync(String name);

    static UserService create() {
        return create(null);
    }

    static UserService create(String url) {
        if (url == null)
            url = ClientServiceProxy.getUrl();

        if (ClientServiceProxy.isEmbedded(url))
            return new com.lealone.test.service.impl.UserServiceImpl();
        else
            return new ServiceProxy(url);
    }

    static class ServiceProxy implements UserService {

        private final JdbcPreparedStatement ps1;
        private final JdbcPreparedStatement ps2;
        private final JdbcPreparedStatement ps3;
        private final JdbcPreparedStatement ps4;
        private final JdbcPreparedStatement ps5;

        private ServiceProxy(String url) {
            ps1 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE ADD(?)");
            ps2 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE FIND(?)");
            ps3 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE UPDATE(?)");
            ps4 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE GET_LIST()");
            ps5 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE USER_SERVICE DELETE(?)");
        }

        @Override
        public Future<Long> addAsync(User user) {
            AsyncCallback<Long> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps1.setObject(1, user.toMap());
                ps1.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("USER_SERVICE.ADD", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        Long ret = rs.getLong(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("USER_SERVICE.ADD", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("USER_SERVICE.ADD", e, ac);
            }
            return ac;
        }

        @Override
        public Future<User> findAsync(String name) {
            AsyncCallback<User> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps2.setString(1, name);
                ps2.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("USER_SERVICE.FIND", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        Object ret = rs.getObject(1);
                        rs.close();
                        ac.setAsyncResult(User.decode(ret));
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("USER_SERVICE.FIND", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("USER_SERVICE.FIND", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Integer> updateAsync(User user) {
            AsyncCallback<Integer> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps3.setObject(1, user.toMap());
                ps3.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("USER_SERVICE.UPDATE", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        Integer ret = rs.getInt(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("USER_SERVICE.UPDATE", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("USER_SERVICE.UPDATE", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Array> getListAsync() {
            AsyncCallback<Array> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps4.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("USER_SERVICE.GET_LIST", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        Array ret = rs.getArray(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("USER_SERVICE.GET_LIST", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("USER_SERVICE.GET_LIST", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Integer> deleteAsync(String name) {
            AsyncCallback<Integer> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps5.setString(1, name);
                ps5.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("USER_SERVICE.DELETE", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        Integer ret = rs.getInt(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("USER_SERVICE.DELETE", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("USER_SERVICE.DELETE", e, ac);
            }
            return ac;
        }
    }
}
