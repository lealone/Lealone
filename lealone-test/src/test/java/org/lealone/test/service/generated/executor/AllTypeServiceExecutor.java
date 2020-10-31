package org.lealone.test.service.generated.executor;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import org.lealone.db.service.ServiceExecutor;
import org.lealone.orm.json.JsonArray;
import org.lealone.orm.json.JsonObject;
import org.lealone.test.orm.generated.User;
import org.lealone.test.service.impl.AllTypeServiceImpl;

/**
 * Service executor for 'all_type_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class AllTypeServiceExecutor implements ServiceExecutor {

    private final AllTypeServiceImpl s = new AllTypeServiceImpl();

    public AllTypeServiceExecutor() {
    }

    @Override
    public String executeService(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "TEST_TYPE":
            ja = new JsonArray(json);
            Integer p_f11 = Integer.valueOf(ja.getValue(0).toString());
            Boolean p_f21 = Boolean.valueOf(ja.getValue(1).toString());
            Byte p_f31 = Byte.valueOf(ja.getValue(2).toString());
            Short p_f41 = Short.valueOf(ja.getValue(3).toString());
            Long p_f51 = Long.valueOf(ja.getValue(4).toString());
            Long p_f61 = Long.valueOf(ja.getValue(5).toString());
            BigDecimal p_f71 = ja.getJsonObject(6).mapTo(BigDecimal.class);
            Double p_f81 = Double.valueOf(ja.getValue(7).toString());
            Float p_f91 = Float.valueOf(ja.getValue(8).toString());
            Time p_f101 = java.sql.Time.valueOf(ja.getValue(9).toString());
            Date p_f111 = java.sql.Date.valueOf(ja.getValue(10).toString());
            Timestamp p_f121 = java.sql.Timestamp.valueOf(ja.getValue(11).toString());
            byte[] p_f131 = ja.getJsonObject(12).mapTo(byte[].class);
            Object p_f141 = ja.getJsonObject(13).mapTo(Object.class);
            String p_f151 = ja.getString(14);
            String p_f161 = ja.getString(15);
            String p_f171 = ja.getString(16);
            Blob p_f181 = ja.getJsonObject(17).mapTo(java.sql.Blob.class);
            Clob p_f191 = ja.getJsonObject(18).mapTo(java.sql.Clob.class);
            UUID p_f201 = java.util.UUID.fromString(ja.getValue(19).toString());
            Array p_f211 = ja.getJsonObject(20).mapTo(java.sql.Array.class);
            User result1 = this.s.testType(p_f11, p_f21, p_f31, p_f41, p_f51, p_f61, p_f71, p_f81, p_f91, p_f101, p_f111, p_f121, p_f131, p_f141, p_f151, p_f161, p_f171, p_f181, p_f191, p_f201, p_f211);
            if (result1 == null)
                return null;
            return JsonObject.mapFrom(result1).encode();
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }
}
