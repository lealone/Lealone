package com.lealone.test.service.generated;

import com.lealone.client.ClientServiceProxy;
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

    List<Object> m1();

    List<Integer> m2();

    Set<Object> m3();

    Set<String> m4();

    Map<Object, Object> m5();

    Map<Integer, String> m6();

    Map<Integer, String> m7(List<Integer> p1, Set<String> p2, Map<Integer, String> p3, Integer p4);

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

        private final PreparedStatement ps1;
        private final PreparedStatement ps2;
        private final PreparedStatement ps3;
        private final PreparedStatement ps4;
        private final PreparedStatement ps5;
        private final PreparedStatement ps6;
        private final PreparedStatement ps7;

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
        public List<Object> m1() {
            try {
                ResultSet rs = ps1.executeQuery();
                rs.next();
                @SuppressWarnings("unchecked")
                List<Object> ret = (List<Object>)rs.getObject(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M1", e);
            }
        }

        @Override
        public List<Integer> m2() {
            try {
                ResultSet rs = ps2.executeQuery();
                rs.next();
                @SuppressWarnings("unchecked")
                List<Integer> ret = (List<Integer>)rs.getObject(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M2", e);
            }
        }

        @Override
        public Set<Object> m3() {
            try {
                ResultSet rs = ps3.executeQuery();
                rs.next();
                @SuppressWarnings("unchecked")
                Set<Object> ret = (Set<Object>)rs.getObject(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M3", e);
            }
        }

        @Override
        public Set<String> m4() {
            try {
                ResultSet rs = ps4.executeQuery();
                rs.next();
                @SuppressWarnings("unchecked")
                Set<String> ret = (Set<String>)rs.getObject(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M4", e);
            }
        }

        @Override
        public Map<Object, Object> m5() {
            try {
                ResultSet rs = ps5.executeQuery();
                rs.next();
                @SuppressWarnings("unchecked")
                Map<Object, Object> ret = (Map<Object, Object>)rs.getObject(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M5", e);
            }
        }

        @Override
        public Map<Integer, String> m6() {
            try {
                ResultSet rs = ps6.executeQuery();
                rs.next();
                @SuppressWarnings("unchecked")
                Map<Integer, String> ret = (Map<Integer, String>)rs.getObject(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M6", e);
            }
        }

        @Override
        public Map<Integer, String> m7(List<Integer> p1, Set<String> p2, Map<Integer, String> p3, Integer p4) {
            try {
                ps7.setObject(1, p1);
                ps7.setObject(2, p2);
                ps7.setObject(3, p3);
                ps7.setInt(4, p4);
                ResultSet rs = ps7.executeQuery();
                rs.next();
                @SuppressWarnings("unchecked")
                Map<Integer, String> ret = (Map<Integer, String>)rs.getObject(1);
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("COLLECTION_TYPE_SERVICE.M7", e);
            }
        }
    }
}
