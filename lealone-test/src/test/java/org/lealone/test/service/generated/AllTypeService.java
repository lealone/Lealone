package org.lealone.test.service.generated;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import org.lealone.client.ClientServiceProxy;
import org.lealone.orm.json.JsonArray;
import org.lealone.orm.json.JsonObject;
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
        else;
            return new Proxy(url);
    }

    User testType(Integer f1, Boolean f2, Byte f3, Short f4, Long f5, Long f6, BigDecimal f7, Double f8, Float f9, Time f10, Date f11, Timestamp f12, byte[] f13, Object f14, String f15, String f16, String f17, Blob f18, Clob f19, UUID f20, Array f21);

    static class Proxy implements AllTypeService {

        private final String url;

        private Proxy(String url) {
            this.url = url;
        }

        @Override
        public User testType(Integer f1, Boolean f2, Byte f3, Short f4, Long f5, Long f6, BigDecimal f7, Double f8, Float f9, Time f10, Date f11, Timestamp f12, byte[] f13, Object f14, String f15, String f16, String f17, Blob f18, Clob f19, UUID f20, Array f21) {
            JsonArray ja = new JsonArray();
            ja.add(f1);
            ja.add(f2);
            ja.add(f3);
            ja.add(f4);
            ja.add(f5);
            ja.add(f6);
            ja.add(f7);
            ja.add(f8);
            ja.add(f9);
            ja.add(f10);
            ja.add(f11);
            ja.add(f12);
            ja.add(f13);
            ja.add(f14);
            ja.add(f15);
            ja.add(f16);
            ja.add(f17);
            ja.add(f18);
            ja.add(f19);
            ja.add(f20);
            ja.add(f21);
            String result = ClientServiceProxy.executeWithReturnValue(url, "ALL_TYPE_SERVICE.TEST_TYPE", ja.encode());
            if (result != null) {
                JsonObject jo = new JsonObject(result);
                return jo.mapTo(User.class);
            }
            return null;
        }
    }
}
