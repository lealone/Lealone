package com.lealone.test.service.generated;

import com.lealone.client.ClientServiceProxy;
import com.lealone.client.jdbc.JdbcPreparedStatement;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Service interface for 'collection_type_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface CollectionTypeService {

    default List<Object> m1() {
        return m1Async().get();
    }

    default List<Integer> m2() {
        return m2Async().get();
    }

    default Set<Object> m3() {
        return m3Async().get();
    }

    default Set<String> m4() {
        return m4Async().get();
    }

    default Map<Object, Object> m5() {
        return m5Async().get();
    }

    default Map<Integer, String> m6() {
        return m6Async().get();
    }

    default Map<Integer, String> m7(List<Integer> p1, Set<String> p2, Map<Integer, String> p3, Integer p4) {
        return m7Async(p1, p2, p3, p4).get();
    }

    Future<List<Object>> m1Async();

    Future<List<Integer>> m2Async();

    Future<Set<Object>> m3Async();

    Future<Set<String>> m4Async();

    Future<Map<Object, Object>> m5Async();

    Future<Map<Integer, String>> m6Async();

    Future<Map<Integer, String>> m7Async(List<Integer> p1, Set<String> p2, Map<Integer, String> p3, Integer p4);

    static CollectionTypeService create() {
        return create(null);
    }

    static CollectionTypeService create(String url) {
        if (url == null)
            url = ClientServiceProxy.getUrl();

        if (ClientServiceProxy.isEmbedded(url))
            return new com.lealone.test.service.impl.CollectionTypeServiceImpl();
        else
            return new ServiceProxy(url);
    }

    static class ServiceProxy implements CollectionTypeService {

        private final JdbcPreparedStatement ps1;
        private final JdbcPreparedStatement ps2;
        private final JdbcPreparedStatement ps3;
        private final JdbcPreparedStatement ps4;
        private final JdbcPreparedStatement ps5;
        private final JdbcPreparedStatement ps6;
        private final JdbcPreparedStatement ps7;

        private ServiceProxy(String url) {
            ps1 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE COLLECTION_TYPE_SERVICE M1()");
            ps2 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE COLLECTION_TYPE_SERVICE M2()");
            ps3 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE COLLECTION_TYPE_SERVICE M3()");
            ps4 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE COLLECTION_TYPE_SERVICE M4()");
            ps5 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE COLLECTION_TYPE_SERVICE M5()");
            ps6 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE COLLECTION_TYPE_SERVICE M6()");
            ps7 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE COLLECTION_TYPE_SERVICE M7(?, ?, ?, ?)");
        }

        @Override
        public Future<List<Object>> m1Async() {
            AsyncCallback<List<Object>> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps1.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M1", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        @SuppressWarnings("unchecked")
                        List<Object> ret = (List<Object>)rs.getObject(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M1", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M1", e, ac);
            }
            return ac;
        }

        @Override
        public Future<List<Integer>> m2Async() {
            AsyncCallback<List<Integer>> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps2.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M2", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        @SuppressWarnings("unchecked")
                        List<Integer> ret = (List<Integer>)rs.getObject(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M2", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M2", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Set<Object>> m3Async() {
            AsyncCallback<Set<Object>> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps3.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M3", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        @SuppressWarnings("unchecked")
                        Set<Object> ret = (Set<Object>)rs.getObject(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M3", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M3", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Set<String>> m4Async() {
            AsyncCallback<Set<String>> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps4.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M4", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        @SuppressWarnings("unchecked")
                        Set<String> ret = (Set<String>)rs.getObject(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M4", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M4", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Map<Object, Object>> m5Async() {
            AsyncCallback<Map<Object, Object>> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps5.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M5", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        @SuppressWarnings("unchecked")
                        Map<Object, Object> ret = (Map<Object, Object>)rs.getObject(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M5", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M5", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Map<Integer, String>> m6Async() {
            AsyncCallback<Map<Integer, String>> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps6.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M6", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        @SuppressWarnings("unchecked")
                        Map<Integer, String> ret = (Map<Integer, String>)rs.getObject(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M6", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M6", e, ac);
            }
            return ac;
        }

        @Override
        public Future<Map<Integer, String>> m7Async(List<Integer> p1, Set<String> p2, Map<Integer, String> p3, Integer p4) {
            AsyncCallback<Map<Integer, String>> ac = ClientServiceProxy.createAsyncCallback();
            try {
                ps7.setObject(1, p1);
                ps7.setObject(2, p2);
                ps7.setObject(3, p3);
                ps7.setInt(4, p4);
                ps7.executeQueryAsync().onComplete(ar -> {
                    if (ar.isFailed()) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M7", ar.getCause(), ac);
                        return;
                    }
                    try {
                        ResultSet rs = ar.getResult();
                        rs.next();
                        @SuppressWarnings("unchecked")
                        Map<Integer, String> ret = (Map<Integer, String>)rs.getObject(1);
                        rs.close();
                        ac.setAsyncResult(ret);
                    } catch (Throwable e) {
                        ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M7", e, ac);
                    }
                });
            } catch (Throwable e) {
                ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M7", e, ac);
            }
            return ac;
        }
    }
}
