package org.lealone.test.service.generated;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import org.lealone.client.ClientServiceProxy;
import org.lealone.db.value.ValueUuid;
import org.lealone.test.orm.generated.User;

/**
 * Service interface for 'all_type_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public interface AllTypeService {

    static AllTypeService create(String url) {
        if (new org.lealone.db.ConnectionInfo(url).isEmbedded())
            return new org.lealone.test.service.impl.AllTypeServiceImpl();
        else
            return new ServiceProxy(url);
    }

    User testType(Integer f1, Boolean f2, Byte f3, Short f4, Long f5, Long f6, BigDecimal f7, Double f8, Float f9, Time f10, Date f11, Timestamp f12, byte[] f13, Object f14, String f15, String f16, String f17, Blob f18, Clob f19, UUID f20, Array f21);

    UUID testUuid(UUID f1);

    static class ServiceProxy implements AllTypeService {

        private final PreparedStatement ps1;
        private final PreparedStatement ps2;

        private ServiceProxy(String url) {
            ps1 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE ALL_TYPE_SERVICE TEST_TYPE(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            ps2 = ClientServiceProxy.prepareStatement(url, "EXECUTE SERVICE ALL_TYPE_SERVICE TEST_UUID(?)");
        }

        @Override
        public User testType(Integer f1, Boolean f2, Byte f3, Short f4, Long f5, Long f6, BigDecimal f7, Double f8, Float f9, Time f10, Date f11, Timestamp f12, byte[] f13, Object f14, String f15, String f16, String f17, Blob f18, Clob f19, UUID f20, Array f21) {
            try {
                ps1.setInt(1, f1);
                ps1.setBoolean(2, f2);
                ps1.setByte(3, f3);
                ps1.setShort(4, f4);
                ps1.setLong(5, f5);
                ps1.setLong(6, f6);
                ps1.setBigDecimal(7, f7);
                ps1.setDouble(8, f8);
                ps1.setFloat(9, f9);
                ps1.setTime(10, f10);
                ps1.setDate(11, f11);
                ps1.setTimestamp(12, f12);
                ps1.setBytes(13, f13);
                ps1.setObject(14, f14);
                ps1.setString(15, f15);
                ps1.setString(16, f16);
                ps1.setString(17, f17);
                ps1.setBlob(18, f18);
                ps1.setClob(19, f19);
                ps1.setBytes(20, ValueUuid.get(f20).getBytes());
                ps1.setArray(21, f21);
                ResultSet rs = ps1.executeQuery();
                rs.next();
                String ret = rs.getString(1);
                rs.close();
                return User.decode(ret);
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("ALL_TYPE_SERVICE.TEST_TYPE", e);
            }
        }

        @Override
        public UUID testUuid(UUID f1) {
            try {
                ps2.setBytes(1, ValueUuid.get(f1).getBytes());
                ResultSet rs = ps2.executeQuery();
                rs.next();
                UUID ret = ValueUuid.get(rs.getBytes(1)).getUuid();
                rs.close();
                return ret;
            } catch (Throwable e) {
                throw ClientServiceProxy.failed("ALL_TYPE_SERVICE.TEST_UUID", e);
            }
        }
    }
}
