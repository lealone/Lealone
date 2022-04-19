/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.orm;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;

import org.junit.Test;
import org.lealone.common.util.Utils;
import org.lealone.db.api.JavaObjectSerializer;
import org.lealone.db.value.ReadonlyArray;
import org.lealone.db.value.ReadonlyBlob;
import org.lealone.db.value.ReadonlyClob;
import org.lealone.test.orm.generated.AllModelProperty;
import org.lealone.test.service.generated.AllTypeService;

public class AllModelPropertyTest extends OrmTestBase {
    @Test
    public void run() {
        Utils.serializer = new MyJavaObjectSerializer();
        SqlScript.createAllModelPropertyTable(this);

        insertLocal();

        AllModelProperty all = AllModelProperty.dao.findOne();
        String str = all.encode();
        all = AllModelProperty.decode(str);
    }

    static class MyJavaObjectSerializer implements JavaObjectSerializer {
        @Override
        public byte[] serialize(Object obj) throws Exception {
            return new byte[] { 0, 1 };
        }

        @Override
        public Object deserialize(byte[] bytes) throws Exception {
            return new Object();
        }
    }

    public static AllModelProperty create(Integer f1, Boolean f2, Byte f3, Short f4, Long f5, Long f6, BigDecimal f7,
            Double f8, Float f9, Time f10, Date f11, Timestamp f12, byte[] f13, Object f14, String f15, String f16,
            String f17, Blob f18, Clob f19, UUID f20, Array f21) {

        AllModelProperty all = new AllModelProperty();

        all.f1.set(f1);
        all.f2.set(f2);
        all.f3.set(f3);
        all.f4.set(f4);
        all.f5.set(f5);
        all.f6.set(f6);
        all.f7.set(f7);
        all.f8.set(f8);
        all.f9.set(f9);

        all.f10.set(f10);
        all.f11.set(f11);
        all.f12.set(f12);
        all.f13.set(f13);
        all.f14.set(f14);
        all.f15.set(f15);
        all.f16.set(f16);
        all.f17.set(f17);

        all.f18.set(f18);
        all.f19.set(f19);
        all.f20.set(f20);
        all.f21.set(f21);

        return all;
    }

    public static void insertRemote(AllTypeService allTypeService) {
        insert(allTypeService);
    }

    public static void insertLocal() {
        insert(null);
    }

    private static void insert(AllTypeService allTypeService) {
        Integer f1 = Integer.valueOf("1");
        Boolean f2 = Boolean.valueOf("true");
        Byte f3 = Byte.valueOf("3");
        Short f4 = Short.valueOf("4");
        Long f5 = Long.valueOf("5");
        Long f6 = Long.valueOf("6");
        BigDecimal f7 = new BigDecimal("7");
        Double f8 = Double.valueOf("8");
        Float f9 = Float.valueOf("9");

        long time = new java.util.Date().getTime();
        Time f10 = new Time(time);
        Date f11 = new Date(time);
        Timestamp f12 = new Timestamp(time);
        byte[] f13 = new byte[] { 13 };
        Object f14 = new Object();
        String f15 = "15";
        String f16 = "16";
        String f17 = "17";

        ReadonlyBlob f18 = new ReadonlyBlob("18");
        ReadonlyClob f19 = new ReadonlyClob("19");
        UUID f20 = UUID.randomUUID();
        ReadonlyArray f21 = new ReadonlyArray(new Object[] { "21", 21 });

        if (allTypeService != null) {
            allTypeService.testType(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18,
                    f19, f20, f21);
        } else {
            AllModelProperty all = create(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17,
                    f18, f19, f20, f21);
            all.insert();
        }
    }
}
