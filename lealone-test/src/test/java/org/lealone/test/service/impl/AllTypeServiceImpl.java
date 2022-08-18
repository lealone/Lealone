/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.service.impl;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;

import org.lealone.test.orm.AllModelPropertyTest;
import org.lealone.test.orm.generated.User;
import org.lealone.test.service.generated.AllTypeService;

public class AllTypeServiceImpl implements AllTypeService {

    @Override
    public User testType(Integer f1, Boolean f2, Byte f3, Short f4, Long f5, Long f6, BigDecimal f7,
            Double f8, Float f9, Time f10, Date f11, Timestamp f12, byte[] f13, Object f14, String f15,
            String f16, String f17, Blob f18, Clob f19, UUID f20, Array f21) {
        AllModelPropertyTest.create(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15,
                f16, f17, f18, f19, f20, f21).insert();
        return new User().name.set("test");
    }

    @Override
    public UUID testUuid(UUID f1) {
        System.out.println(f1);
        f1 = UUID.randomUUID();
        System.out.println(f1);
        return f1;
    }
}
